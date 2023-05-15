package frontend.service

import akka.actor.ActorSystem
import akka.grpc.scaladsl.WebHandler
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.Materializer
import backend.CBackend
import com.typesafe.config.ConfigFactory
import core.materialization.{PresetMaterializationStrategy, RandomizedMaterializationStrategy, SchemaBasedMaterializationStrategy}
import core.solver.SolverTools
import core.solver.moment.CoMoment5SolverDouble
import core.{DataCube, PartialDataCube}
import frontend.generators._
import frontend.schema._
import frontend.schema.encoders.{ColEncoder, DynamicColEncoder}
import frontend.service.GetRenameTimeResponse.ResultRow
import frontend.service.SelectDataCubeForQueryResponse.DimHierarchy
import planning.NewProjectionMetaData
import util.{BitUtils, Profiler, Util}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.io.File
//#grpc-web


object SudokubeServer {
  def main(args: Array[String]): Unit = {
    // important to enable HTTP/2 in ActorSystem's config
    val conf =
      ConfigFactory.parseString("akka.http.server.enable-http2 = on").withFallback(ConfigFactory.defaultApplication())
    implicit val sys: ActorSystem = ActorSystem("HelloWorld", conf)
    implicit val ec: ExecutionContext = sys.dispatcher

    //#concatOrNotFound
    // explicit types not needed but included in example for clarity
    val sudokubeService: PartialFunction[HttpRequest, Future[HttpResponse]] =
    SudokubeServiceHandler.partial(new SudokubeServiceImpl())

    //#grpc-web
    val grpcWebServiceHandlers = WebHandler.grpcWebHandler(sudokubeService)

    Http()
      .newServerAt("localhost", 8081)
      .bind(grpcWebServiceHandlers)
      //#grpc-web
      .foreach { binding => println(s"gRPC-Web server bound to: ${binding.localAddress}") }
  }
}

case class MyDimLevel(name: String, enc: ColEncoder[_]) {
  def decodePrefix(enc: ColEncoder[_], numBits: Int): IndexedSeq[String] = {
    if (numBits == 0) {
      Vector("(all)")
    }
    else if (numBits == enc.bits.length) {
      (0 to enc.maxIdx).map(i => enc.decode_locally(i).toString)
    } else {
      val droppedBits = enc.bits.length - numBits
      (0 to enc.maxIdx).groupBy(_ >> droppedBits).toVector.sortBy(_._1).map{ case (grp, idxes) =>
        val first = enc.decode_locally(idxes.min).toString
        val last = enc.decode_locally(idxes.max).toString
        first + " to " + last
      }.toVector
    }
  }
  def values(numBits: Int) = decodePrefix(enc, numBits)
  def fullName(numBits: Int) = {
    val grp = if (numBits == bits.size) ""
    else "/" + (1 << (bits.size - numBits))
    name + grp
  }
  val bits = enc.bits
  def selectedBits(numBits: Int) = bits.takeRight(numBits)
}

case class MyDimHierarchy(name: String, levels: IndexedSeq[MyDimLevel]) {
  lazy val flattenedLevels = {
    levels.flatMap { l =>
      val totalbits = l.bits.size
      (0 to totalbits).map { nb => l.fullName(nb) -> (l, nb) }
    }
  }
  lazy val flattenedLevelsMap = flattenedLevels.toMap
  //def currentLevel(l) = levels(levelIdx)
  //def increment = {
  //  if (currentLevel.numBits < currentLevel.bits.length)
  //    currentLevel.numBits += 1
  //  else {
  //    if (levelIdx < levels.size - 1) {
  //      levelIdx += 1
  //      currentLevel.numBits = 0
  //    }
  //  }
  //}
  //def decrement = {
  //  if (currentLevel.numBits > 0) {
  //    currentLevel.numBits -= 1
  //  } else {
  //    if (levelIdx > 0) {
  //      levelIdx -= 1
  //      currentLevel.numBits = currentLevel.bits.size
  //    }
  //  }
  //}
}

class SudokubeServiceImpl(implicit mat: Materializer) extends SudokubeService {
  implicit val backend = CBackend.default
  implicit val ec = ExecutionContext.global
  def getDimHierarchy(dim: Dim2): Vector[MyDimHierarchy] = dim match {
    case BD2(rootname, children, cross) => if (cross) {
      children.map(c => getDimHierarchy(c)).reduce(_ ++ _)
    } else {
      //assuming all children are LD2 here
      val levels = children.map { case LD2(name, encoder) =>
        MyDimLevel(name, encoder)
      }
      val hierarchy = MyDimHierarchy(rootname, levels)
      Vector(hierarchy)
    }
    case LD2(name, encoder) =>
      val level = MyDimLevel(name, encoder)
      val hierarchy = MyDimHierarchy(name, Vector(level))
      Vector(hierarchy)
    case r@DynBD2() => r.children.values.map(c => getDimHierarchy(c)).reduce(_ ++ _)
  }
  def getCubeGenerator(cname: String) = {
    cname match {
      case "SSB" => new SSB(100)
      case "NYC" => new NYC()
      case "WebShopSales" => new WebshopSales()
      case "WebShopDyn" => new WebShopDyn()
      case "TinyData" => new TinyData()
    }
  }
  def cuboidToDimSplit(cub: Seq[Int], cols: Vector[LD2[_]]) = {
    //println("Split cuboid " + cub)
    //cols.map{case LD2(n, e) =>
    //  val bits = e match {
    //    case d:DynamicColEncoder[_] => d.bits + " + " + d.isNotNullBit
    //    case _ => e.bits
    //  }
    //  println(n -> bits)
    //}
    val groups = cub.groupBy(b => cols.find(l => l.encoder.bits.contains(b)).get)
    groups.map { case (ld, bs) =>
      ld.name -> ld.encoder.bits.reverse.map(b => bs.contains(b))
    }
  }

  object MaterializeState {
    var cg: CubeGenerator = null
    var baseCuboid: DataCube = null
    //TODO: Merge into common parent
    var schema: Schema2 = null
    var columnMap: Map[String, ColEncoder[_]] = null
    val chosenCuboids = ArrayBuffer[IndexedSeq[Int]]()
    val shownCuboids = ArrayBuffer[IndexedSeq[Int]]()
    val shownCuboidsManualView = ArrayBuffer[(IndexedSeq[Int], Boolean)]()
  }

  object ExploreState {
    var dc: DataCube = null
    var sch: DynamicSchema2 = null
    var columnMap: Map[String, DynamicColEncoder[_]] = null
    val timeDimension: String = "RowId"
    var totalTimeBits = 5
    var exploreResult = 0 //0 -> No info, 1 -> ColIsAdded, 2 -> ColIsDeleted
    val sliceArray = collection.mutable.ArrayBuffer[Boolean]() //0 in this array refers to the leftmost, most significant bit in dimension
    var colNotNullBit = 0
    var timeRange = Array(0, 0, 0, 0)
  }

  object QueryState {
    var dc: DataCube = null
    var sch: Schema2 = null
    var hierarchy: Vector[MyDimHierarchy] = null
    var columnMap: Map[String, Map[String, (MyDimLevel, Int)]] = null
    val stats = collection.mutable.Map[String, Double]() withDefaultValue(0.0)
    var shownSliceValues: IndexedSeq[(String, Int, Boolean)] = null //value, original id, and isSelected
    var cubsFetched = 0
    var prepareCuboids = Seq[NewProjectionMetaData]()
    val filters = ArrayBuffer[(String, String, ArrayBuffer[(String, Int)])]()
    var filterCurrentDimArgs: (String, String) = null
    var momentSolver: CoMoment5SolverDouble = null
    var sortedQuery = IndexedSeq[Int]()
    var computeSortedIdx: ((Int, Int) => Int) = null
    var diceBits = Seq[Int]()
    var diceArgs = Seq[Seq[Int]]()
    var validXvalues = Seq[(String, Int)]()
    var validYvalues = Seq[(String, Int)]()
  }

  def bitsToBools(total: Int, cuboidBits: Set[Int]) = {
    (0 until total).reverse.map { b => cuboidBits contains b }
  }
  /* Materialize */
  override def getBaseCuboids(in: Empty): Future[BaseCuboidResponse] = {
    Future.successful(BaseCuboidResponse(List("WebShop", "NYC", "SSB", "WebShopDyn", "TinyData")))
  }
  override def selectBaseCuboid(in: SelectBaseCuboidArgs): Future[SelectBaseCuboidResponse] = {
    import MaterializeState._
    import SelectBaseCuboidResponse._
    val dsname = in.cuboid
    println("SelectBaseCuboid arg:" + in)
    Future {
      cg = getCubeGenerator(dsname)
      schema = cg.schemaInstance
      val dims = schema.columnVector.map { case LD2(name, enc) => CuboidDimension(name, enc.bits.size) }
      columnMap = schema.columnVector.map { case LD2(name, enc) => name -> enc }.toMap
      baseCuboid = cg.loadBase()
      shownCuboids.clear()
      shownCuboidsManualView.clear()
      chosenCuboids.clear()
      println("Base cuboid" + baseCuboid.cubeName + "  loaded")
      val res = SelectBaseCuboidResponse(dims)
      println("\t response: " + res)
      res
    }
  }

  override def selectMaterializationStrategy(in: SelectMaterializationStrategyArgs): Future[Empty] = {
    import MaterializeState._
    println("SelectMaterializationStrategy arg: " + in)
    val args = in.args.map(_.toInt).toVector
    val ms = in.name match {
      case "Randomized" => new RandomizedMaterializationStrategy(baseCuboid.index.n_bits, args(0), args(1))
      case "Prefix" => new SchemaBasedMaterializationStrategy(schema, args(0), args(1))
    }
    chosenCuboids.clear()
    chosenCuboids ++= ms.projections
    println("\t response: OK")
    Future.successful(Empty())
  }

  override def getChosenCuboids(in: GetCuboidsArgs): Future[GetChosenCuboidsResponse] = {
    import MaterializeState._
    println("GetChosenCuboids  arg: " + in)
    val cubsInPage = in.rowsPerPage
    def filterCondition(cub: IndexedSeq[Int]) = {
      val cubSet = cub.toSet
      in.filters.map { f =>
        val filterBits = columnMap(f.dimensionName).bits.slice(f.bitsFrom, f.bitsTo).toSet
        filterBits.subsetOf(cubSet)
      }.fold(cub.size < baseCuboid.index.n_bits)(_ && _) //exclude base cuboid
    }
    val filteredCuboids = chosenCuboids.filter(filterCondition(_))
    val numCuboids = filteredCuboids.size
    val requestedCuboidsEnd = ((in.requestedPageId + 1) * cubsInPage) min numCuboids //exclusive
    val requestedCuboidsStart = (requestedCuboidsEnd - cubsInPage) max 0
    shownCuboids.clear()
    shownCuboids ++= filteredCuboids.slice(requestedCuboidsStart, requestedCuboidsEnd)
    val cubs = shownCuboids.map { cub =>
      val dims = cuboidToDimSplit(cub, schema.columnVector).map { case (k, v) => DimensionBits(k, v) }.toSeq
      CuboidDef(dims)
    }
    val res = GetChosenCuboidsResponse(cubs)
    println("\t response: " + res)
    Future.successful(res)
  }

  override def deleteChosenCuboid(in: DeleteSelectedCuboidArgs): Future[Empty] = {
    import MaterializeState._
    println("DeleteChosenCuboid arg: " + in)
    val id = in.cuboidIdWithinPage
    val deletedCuboid = shownCuboids(id)
    shownCuboids -= deletedCuboid
    chosenCuboids -= deletedCuboid
    println("\t response: OK")
    Future.successful(Empty())
  }
  override def getAvailableCuboids(in: GetCuboidsArgs): Future[GetAvailableCuboidsResponse] = {
    import MaterializeState._
    println("GetAvailableCuboids " + in)
    Future {
      val cubsInPage = in.rowsPerPage
      val nbits = baseCuboid.index.n_bits
      val filterBits = in.filters.map { f => columnMap(f.dimensionName).bits.slice(f.bitsFrom, f.bitsTo) }.fold(Vector())(_ ++ _)
      val bitsToPick = (0 until nbits).diff(filterBits)
      val n2 = bitsToPick.size
      var requestedCubStart = BigInt(in.rowsPerPage * in.requestedPageId)
      var nextNumCuboids = cubsInPage
      var k = 0
      var binom = BigInt(1)
      while (binom <= requestedCubStart && k < n2) {
        requestedCubStart -= binom
        binom *= (n2 - k)
        binom /= (k + 1)
        k += 1
      }
      val availableCuboidsView = ArrayBuffer[IndexedSeq[Int]]()
      while (binom <= (requestedCubStart + nextNumCuboids) && k < n2) {
        val cubs = bitsToPick.combinations(k).slice(requestedCubStart.toInt, binom.toInt).map(c => c ++ filterBits)
        availableCuboidsView ++= cubs
        nextNumCuboids -= cubs.size
        requestedCubStart = 0
        binom *= (n2 - k)
        binom /= (k + 1)
        k += 1
      }
      //TODO: FIX: Sometimes there are twice as many available cuboids
      if (k < n2) //do not pick all bits
        availableCuboidsView ++= bitsToPick.combinations(k).slice(requestedCubStart.toInt, (requestedCubStart + nextNumCuboids).toInt).map(c => c ++ filterBits)
      shownCuboidsManualView.clear()
      shownCuboidsManualView ++= availableCuboidsView.map { cub =>
        val isChosen = chosenCuboids.contains(cub)
        cub -> isChosen
      }
      val cubs = shownCuboidsManualView.map { case (cub, isC) =>
        val dims = cuboidToDimSplit(cub, schema.columnVector).map { case (k, v) => DimensionBits(k, v) }.toSeq
        GetAvailableCuboidsResponse.ManualSelectionCuboidDef(dims, isC)
      }
      val res = GetAvailableCuboidsResponse(cubs)
      println("\t response: " + res)
      res
    }
  }
  override def manuallyUpdateCuboids(in: ManuallyUpdateCuboidsArgs): Future[Empty] = {
    import MaterializeState._
    println("ManuallyUpdateCuboids arg: " + in)
    in.isChosen.zip(shownCuboidsManualView).foreach { case (afterChosen, (cub, beforeChosen)) =>
      if (beforeChosen && !afterChosen)
        chosenCuboids -= cub
      else if (!beforeChosen && afterChosen)
        chosenCuboids += cub
    }
    println("\t response: OK")
    Future.successful(Empty())
  }
  override def materializeCuboids(in: MaterializeArgs): Future[Empty] = {
    import MaterializeState._
    println("MaterializeCuboids arg: " + in)
    Future {
      val nbits = baseCuboid.index.n_bits
      val ms = PresetMaterializationStrategy(nbits, chosenCuboids :+ (0 until nbits)) //always add base cuboid
      val dc = new PartialDataCube(cg.inputname + "_" + in.cubeName, baseCuboid.cubeName)
      dc.buildPartial(ms)
      dc.save()
      println("\t response: OK")
      Empty()
    }
  }

  /* Explore */
  override def getDataCubesForExplore(in: Empty): Future[GetCubesResponse] = {
    println("GetDataCubes Explore")
    val file = File("cubedata")
    val potentialCubes = file.toDirectory.dirs.map(_.name)
    val dynamicCubes = potentialCubes.filter { fn =>
      File(s"cubedata/$fn/$fn.sch").exists
    } //only dynamic schema
    //Give WebshopDyn as first entry
    val response = GetCubesResponse(("WebShopDyn_base" +: dynamicCubes.toVector).distinct)
    Future.successful(response)
  }
  override def selectDataCubeForExplore(in: SelectDataCubeArgs): Future[SelectDataCubeForExploreResponse] = {
    import ExploreState._
    Future {
      val cgName = in.cube.split("_")(0)
      val cg = getCubeGenerator(cgName)
      dc = if (in.cube.endsWith("_base")) {
        cg.loadBase()
      } else {
        PartialDataCube.load(in.cube, cg.baseName)
      }
      sch = cg.schemaInstance.asInstanceOf[DynamicSchema2]
      columnMap = sch.columnVector.map { case LD2(name, enc) => name -> enc.asInstanceOf[DynamicColEncoder[_]] }.toMap
      totalTimeBits = columnMap(timeDimension).bits.size
      SelectDataCubeForExploreResponse(columnMap.keys.toVector)
    }
  }
  override def isRenameQuery(in: IsRenamedQueryArgs): Future[IsRenamedQueryResponse] = {
    import ExploreState._
    Future {
      val bit1 = columnMap(in.dimension1).isNotNullBit
      val bit0 = columnMap(in.dimension2).isNotNullBit

      val qu = Vector(bit0, bit1)
      val qs = qu.sorted
      val permf = Util.permute_unsortedIdx_to_sortedIdx(qu)
      val sortedres = dc.naive_eval(qs).map(_.toInt)
      val result = sortedres.indices.map { ui =>
        val si = permf(ui)
        sortedres(si)
      }
      val isRenamed = (result(0) == 0) && (result(3) == 0)
      val response = IsRenamedQueryResponse(result, isRenamed)
      response
    }
  }

  def doRenameTimeQuery = {
    import ExploreState._
    Future {
      val bit0 = colNotNullBit
      val timeBitsInQuery = columnMap(timeDimension).bits.takeRight(sliceArray.size + 1)
      val fullquery = bit0 +: timeBitsInQuery
      println("RenameTime query = " + fullquery)
      val permf = Util.permute_unsortedIdx_to_sortedIdx(fullquery)
      val sortedRes = dc.naive_eval(fullquery.sorted).map(_.toInt)
      val unsortedRes = sortedRes.indices.map { ui =>
        val si = permf(ui)
        sortedRes(si)
      }.toArray
      val sliceArgs = sliceArray.zipWithIndex.map { case (sv, i) => (fullquery.size - 1 - i) -> (if (sv) 1 else 0) }
      val slicedRes = Util.slice(unsortedRes, sliceArgs)
      println("RenameTime queryRes = " + slicedRes.toVector)
      val row0 = ResultRow(timeRange(0) + "-" + timeRange(1), slicedRes(0), slicedRes(1))
      val row1 = ResultRow(timeRange(2) + "-" + timeRange(3), slicedRes(2), slicedRes(3))
      val zeroIndices = slicedRes.indices.filter(i => slicedRes(i) == 0).toSet
      var isContinue = true
      assert(slicedRes.size == 4)
      val currentSlice = sliceArray.toVector
      val currentTimeRange = timeRange.clone()
      if (zeroIndices.isEmpty) {
        isContinue = false
      }
      else if (Set(0, 2).subsetOf(zeroIndices)) isContinue = false //no rename
      //(0,1) and (2,3) both zero not possible. (1,3) zero means all null
      else if (Set(0, 3).subsetOf(zeroIndices)) {
        isContinue = false
        exploreResult = 2 //column deleted

      } else if (Set(1, 2).subsetOf(zeroIndices)) {
        isContinue = false
        exploreResult = 1 //column added
      } else if (zeroIndices.contains(0)) {
        exploreResult = 2 //col deleted somewhere in 1 slice
        sliceArray += true
        timeRange(0) = timeRange(2)
        timeRange(1) = (timeRange(3) + timeRange(0) - 1) / 2
        timeRange(2) = (timeRange(3) + timeRange(0) + 1) / 2
      } else if (zeroIndices.contains(1)) {
        exploreResult = 1 // col added somewhere in 1-slice
        sliceArray += true
        timeRange(0) = timeRange(2)
        timeRange(1) = (timeRange(3) + timeRange(0) - 1) / 2
        timeRange(2) = (timeRange(3) + timeRange(0) + 1) / 2
      } else if (zeroIndices.contains(2)) {
        exploreResult = 1 //col added in 0-slice
        sliceArray += false
        timeRange(3) = timeRange(1)
        timeRange(1) = (timeRange(3) + timeRange(0) - 1) / 2
        timeRange(2) = (timeRange(3) + timeRange(0) + 1) / 2
      } else if (zeroIndices.contains(3)) {
        exploreResult = 2 //col deleted in 0-slice
        sliceArray += false
        timeRange(3) = timeRange(1)
        timeRange(1) = (timeRange(3) + timeRange(0) - 1) / 2
        timeRange(2) = (timeRange(3) + timeRange(0) + 1) / 2
      }
      val string = if (!isContinue) {
        exploreResult match {
          case 0 => "No renaming could be detected within the available resolution"
          case 1 => s"Column possibly added between ${currentTimeRange(1)} and ${currentTimeRange(2)}"
          case 2 => s"Column possibly deleted between ${currentTimeRange(1)} and ${currentTimeRange(2)}"
        }
      } else {
        if (sliceArray.last) "Slicing to upper range" else "Slicing to lower range"
      }
      println("RenameTime currentTimeRange = " + currentTimeRange.toVector + "   nextTimeRange = " + timeRange.toVector)
      println(string)
      GetRenameTimeResponse(totalTimeBits, !isContinue, currentSlice, Vector(row0, row1), string)
    }
  }
  override def startRenameTimeQuery(in: GetRenameTimeArgs): Future[GetRenameTimeResponse] = {
    import ExploreState._
    exploreResult = 0
    sliceArray.clear()
    timeRange(0) = 0
    timeRange(3) = (1 << totalTimeBits) - 1
    timeRange(1) = (timeRange(0) + timeRange(3) - 1) / 2
    timeRange(2) = (timeRange(0) + timeRange(3) + 1) / 2
    colNotNullBit = columnMap(in.dimensionName).isNotNullBit
    doRenameTimeQuery
  }
  override def continueRenameTimeQuery(in: Empty): Future[GetRenameTimeResponse] = {
    doRenameTimeQuery
  }
  override def transformCube(in: TransformDimensionsArgs): Future[Empty] = {
    Future.successful(Empty())
  }

  /** Query */
  override def getDataCubesForQuery(in: Empty): Future[GetCubesResponse] = {
    println("GetDataCubes Explore")
    val file = File("cubedata")
    val potentialCubes = file.toDirectory.dirs.map(_.name)
    //Give Webshop as first entry
    val response = GetCubesResponse(("WebShopSales_base" +: potentialCubes.toVector).distinct)
    Future.successful(response)
  }

  override def selectDataCubeForQuery(in: SelectDataCubeArgs): Future[SelectDataCubeForQueryResponse] = {
    import QueryState._
    Future {
      println("SelectDataCubeForQuery arg:" + in)
      val cgName = in.cube.split("_")(0)
      val cg = getCubeGenerator(cgName)
      //filters.clear()
      dc = if (in.cube.endsWith("_base")) {
        cg.loadBase()
      } else {
        PartialDataCube.load(in.cube, cg.baseName)
      }
      dc.loadPrimaryMoments(cg.baseName)
      sch = cg.schemaInstance
      sch.initBeforeDecode()
      hierarchy = getDimHierarchy(sch.root)
      columnMap = hierarchy.map { h => h.name -> h.flattenedLevelsMap }.toMap
      val dims = hierarchy.map { h => DimHierarchy(h.name, h.flattenedLevels.map(_._1)) }
      val cuboidDims = sch.columnVector.map { case LD2(name, encoder) => CuboidDimension(name, encoder.bits.size) }
      val res = SelectDataCubeForQueryResponse(dims, cuboidDims, Vector(cg.measureName))
      println("\t response:" + res)
      res
    }
  }
  override def getValuesForSlice(in: GetSliceValuesArgs): Future[GetSliceValueResponse] = {
    import QueryState._
    println("GetValuesForSlice arg: " + in)
    val dname = in.dimensionName
    val lname = in.dimensionLevel
    val (level, numBits) = columnMap(dname)(lname)
    val allValuesWithIndex = level.values(numBits).zipWithIndex.iterator
    val fileteredValues = allValuesWithIndex.filter(_._1.contains(in.searchText))
    val rowStart = in.requestedPageId * in.numRowsInPage
    val rowEnd = (in.requestedPageId + 1) * in.numRowsInPage
    shownSliceValues = fileteredValues.slice(rowStart, rowEnd).toVector.map { case (v, i) => (v, i, false) }
    filterCurrentDimArgs = (dname, lname)
    val response = GetSliceValueResponse(shownSliceValues.map(_._1), shownSliceValues.map(_._3))
    println("\t response: " + response)
    Future.successful(response)
  }

  override def getFilters(in: Empty): Future[GetFiltersResponse] = {
    import QueryState._
    println("GetFilters")
    val response = GetFiltersResponse(filters.map { f => GetFiltersResponse.FilterDef(f._1, f._2, f._3.map(_._1).mkString(";")) })
    println("\t response : " + response)
    Future.successful(response)
  }

  override def setValuesForSlice(in: SetSliceValuesArgs): Future[Empty] = {
    import QueryState._
    println("SetValueForSlice arg: " + in)
    val filter = filters.find(f => (f._1 == filterCurrentDimArgs._1) && (f._2 == filterCurrentDimArgs._2))
    val filterAB = if (filter.isEmpty) {
      val ab = new ArrayBuffer[(String, Int)]()
      filters += ((filterCurrentDimArgs._1, filterCurrentDimArgs._2, ab))
      ab
    } else {
      filter.get._3
    }
    shownSliceValues.zip(in.isSelected).foreach { case ((v, i, before), after) =>
      if (!before && after) {
        filterAB += ((v, i))
      } else if (before && !after) {
        filterAB -= ((v, i))
      }
    }
    println("currently selected indexes for slice = " + filterAB)
    println("\t response: OK")
    Future.successful(Empty())
  }

  override def deleteFilter(in: DeleteFilterArgs): Future[Empty] = {
    import QueryState._
    println("DeleteFilter arg: " + in)
    filters.remove(in.index)
    println("\t response: OK")
    Future.successful(Empty())
  }

  def runQuery(): Future[QueryResponse] = {
    import QueryResponse._
    import QueryState._

    Future {
      val nextCuboidToFetch = prepareCuboids(cubsFetched)

      val fetchedData = Profiler("Fetch") { dc.fetch2[Double](List(nextCuboidToFetch)) }

      val sortedRes = Profiler("Solve") {
        momentSolver.add(nextCuboidToFetch.queryIntersection, fetchedData)
        momentSolver.fillMissing()
        momentSolver.solve()
      }
      println("SortedRes = " + sortedRes.mkString(" "))
      println("DiceBits = " + diceBits)
      println("DiceArgs = " + diceArgs)
      val diceRes = Util.dice(sortedRes, diceBits, diceArgs)
      println("diceRes = " + diceRes.mkString(" "))
      val series = validYvalues.reverse.map{case (ylabel, yid) => //we reverse the order of series
          SeriesData(ylabel, validXvalues.map{case (xlabel, xid) =>
          val si = computeSortedIdx(xid, yid)
           XYPoint(xlabel, diceRes(si).toFloat)
          })
      }
      println("AllSeries")
      series.foreach(println)
      stats("PrepareTime") += Profiler.getDurationMicro("Prepare")
      stats("FetchTime") += Profiler.getDurationMicro("Fetch")
      stats("SolveTime") += Profiler.getDurationMicro("Solve")
      stats("DOF") = momentSolver.dof

      val cubsWithinPage = 10 //FIXME
      val shownCuboids = Await.result(getPreparedCuboids(GetPreparedCuboidsArgs(0, cubsWithinPage)), Duration.Inf).cuboids //FIXME: Remove
      val statsToShow = stats.map(p => new QueryStatistic(p._1, p._2.toString)).toSeq
      val fetchedCuboidIDwithinPage = cubsFetched % cubsWithinPage
      cubsFetched += 1
      val isComplete = cubsFetched == prepareCuboids.size
      val response = QueryResponse(0, shownCuboids, fetchedCuboidIDwithinPage, isComplete, series, statsToShow) // FIXME: change 0 to page id of current cuboid
      response
    }
  }
  def extractValidLabelsAndIndexesForDims(dims : Seq[QueryArgs.DimensionDef]) = {
    import QueryState._
    println("Processing labels for " + dims)
    val isSoloDim = dims.size == 1
    val foldResult = if(dims.isEmpty)
      Vector("(all)" -> 0) -> 0
    else
      dims.map { d =>
      val (level, numBits) = columnMap(d.dimensionName)(d.dimensionLevel)
      val validValues = level.values(numBits).zipWithIndex
      (d.dimensionLevel, validValues, numBits)
    }.foldLeft(IndexedSeq("" -> 0), 0) { case ((accvv, accbits), (dname, curvv, curbits)) =>
      curvv.flatMap { case (curv, curi) =>
        accvv.map { case (accv, acci) =>
          //put curi as higherorder bits infront of acci
          //Put curv infront of accv
          val newi = (curi << accbits) + acci
          val newv = (if(isSoloDim) curv else s"$dname=$curv ") + accv
          (newv, newi)
        }
      } -> (accbits + curbits)
    }//labels and indexes of valid rows within (0 .. numEntries)
    val bits = dims.map { d =>
      val (level, numBits) = columnMap(d.dimensionName)(d.dimensionLevel)
      level.selectedBits(numBits)
    }.flatten
    (bits, foldResult._1, (1 << foldResult._2))
  }
  override def startQuery(in: QueryArgs): Future[QueryResponse] = {
    import QueryState._
    stats.clear()
    cubsFetched = 0
    println("StartQuery arg:" + in)
    Profiler.resetAll()
    val (xbits, xlabels, xtotal) = extractValidLabelsAndIndexesForDims(in.horizontal.reverse) //treat left most as most-significant
    val (ybits, ylabels, ytotal) = extractValidLabelsAndIndexesForDims(in.series.reverse)
    validXvalues = xlabels
    validYvalues = ylabels
    println("Valid x = " + xlabels)
    println("Valid y = " + ylabels)
    val diceBitsAndArgs = filters.map { case (dname, lname, ab) =>
      val (level, numBits) = columnMap(dname)(lname)
      val bits = level.selectedBits(numBits)
      val idxes = ab.map { case (_, idx) =>
        var idx2 = idx
        val res = bits.map { b => //LSB to MSB
          val v = idx2 & 1
          idx2 >>= 1
          v
        }
        res
      }
      (bits, idxes)
    }

    val zbits = diceBitsAndArgs.map(_._1).foldLeft(Vector[Int]())(_ ++ _)
    diceArgs = diceBitsAndArgs.map(_._2).foldLeft(Vector(Vector[Int]())){ case(accvss, curvss) =>
      accvss.flatMap{ accvs =>
        curvss.map{ curvs =>
          accvs ++ curvs
        }
      }
    }
    val aggQuery = (xbits ++ ybits).toVector
    val fullQuery = (aggQuery ++ zbits).toVector
    sortedQuery = fullQuery.sorted
    println(s"aggQuery $aggQuery, fullQuery = $fullQuery, sorted = $sortedQuery")
    diceBits = zbits.map{b => sortedQuery.indexOf(b)} //Dice is done on the sorted result before permuting
    val permf =  Util.permute_unsortedIdx_to_sortedIdx(aggQuery) //slice is already done, so only aggdims remain
    computeSortedIdx = (xid: Int, yid: Int) => {
      val ui = yid * xtotal + xid
      val si = permf(ui)
      println(s"x=$xid y=$yid ui=$ui si=$si")
      si
    }

    val isBatch = false //in.isBatchMode) //FIXME Always in online mode for now

    val numSeries =
      prepareCuboids = Profiler("Prepare") {
      if (isBatch)
        dc.index.prepareBatch(sortedQuery)
      else
        dc.index.prepareOnline(sortedQuery, 2)
    }
    val pm = Profiler("Prepare") {
      SolverTools.preparePrimaryMomentsForQuery[Double](sortedQuery, dc.primaryMoments)
    }
    momentSolver = Profiler("Solve") {
      new CoMoment5SolverDouble(sortedQuery.size, isBatch, null, pm)
    }
    runQuery()
  }
  override def continueQuery(in: Empty): Future[QueryResponse] = {
    import QueryState._
    runQuery()
  }

  override def getPreparedCuboids(in: GetPreparedCuboidsArgs): Future[GetPreparedCuboidsResponse] = {
    import QueryState._
    val start = in.numRowsInPage * in.requestedPageId
    val end = in.numRowsInPage * (in.requestedPageId + 1)
    val cuboidsToDisplay = prepareCuboids.slice(start, end).map { pm =>
      val cub = BitUtils.IntToSet(pm.queryIntersection).map{i => sortedQuery(i)}
      val dims = cuboidToDimSplit(cub, sch.columnVector).map { case (k, v) => DimensionBits(k, v) }.toSeq
      CuboidDef(dims)
    }
    val response = GetPreparedCuboidsResponse(cuboidsToDisplay)
    Future.successful(response)
  }
}
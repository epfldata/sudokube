package frontend.service

import akka.actor.ActorSystem
import akka.grpc.GrpcServiceException
import akka.grpc.scaladsl.WebHandler
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.settings.ServerSettings
import akka.stream.Materializer
import backend.CBackend
import com.typesafe.config.ConfigFactory
import core.materialization.{PresetMaterializationStrategy, RandomizedMaterializationStrategy, SchemaBasedMaterializationStrategy}
import core.solver.iterativeProportionalFittingSolver.NewVanillaIPFSolver
import core.solver.lpp.SliceSparseSolver
import core.solver.moment.CoMoment5SolverDouble
import core.solver.{NaiveSolver, Rational, SolverTools}
import core.{AbstractDataCube, DataCube, MultiDataCube, PartialDataCube}
import frontend._
import frontend.cubespec.Aggregation._
import frontend.cubespec.{CompositeMeasure, CountMeasure, Measure}
import frontend.generators._
import frontend.schema._
import frontend.schema.encoders._
import frontend.service.GetRenameTimeResponse.ResultRow
import frontend.service.SelectDataCubeForQueryResponse.DimHierarchy
import io.grpc.Status
import planning.NewProjectionMetaData
import util.{BitUtils, Profiler, Util}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.io.File
//#grpc-web


object SudokubeServer {
  def main(args: Array[String]): Unit = {
    // important to enable HTTP/2 in ActorSystem's config
    val conf =
      ConfigFactory.parseString("akka.http.server.enable-http2 = on")
        .withFallback(ConfigFactory.defaultApplication())
    implicit val sys: ActorSystem = ActorSystem("HelloWorld", conf)
    implicit val ec: ExecutionContext = sys.dispatcher

    //#concatOrNotFound
    // explicit types not needed but included in example for clarity
    val sudokubeService: PartialFunction[HttpRequest, Future[HttpResponse]] =
    SudokubeServiceHandler.partial(new SudokubeServiceImpl())
    //#grpc-web

    val grpcWebServiceHandlers = WebHandler.grpcWebHandler(sudokubeService)
    Http()
      .newServerAt("0.0.0.0", 8081)
      .adaptSettings(x => x.withTimeouts(x.timeouts.withRequestTimeout(Duration.Inf)))
      .bind(grpcWebServiceHandlers)
      //#grpc-web
      .foreach { binding => println(s"gRPC-Web server bound to: ${binding.localAddress}") }
  }
}

case class MyDimLevel(name: String, enc: ColEncoder[_]) {
  def decodePrefix(enc: ColEncoder[_], numBits: Int): IndexedSeq[(String, Int)] = {
    if (numBits == 0) {
      Vector("(all)" -> 0)
    }
    else
      enc match {
        case se: StaticColEncoder[_] =>
          if (numBits == enc.bits.length) {
            (0 to enc.maxIdx).map(i => enc.decode_locally(i).toString).zipWithIndex
          } else {
            val droppedBits = enc.bits.length - numBits
            (0 to enc.maxIdx).groupBy(_ >> droppedBits).toVector.sortBy(_._1).map { case (grp, idxes) =>
              val first = enc.decode_locally(idxes.min).toString
              val last = enc.decode_locally(idxes.max).toString
              first + " to " + last
            }.zipWithIndex
          }
        case _: MergedMemColEncoder[_] | _: DynamicColEncoder[_] =>

          val localOffset = 1 << (numBits - 1)
          if (numBits == 1) {
            Vector("NULL", "Not NULL").zipWithIndex
          } else if (numBits - 1 == enc.bits.length) {
            ("NULL" -> 0) +: (0 to enc.maxIdx).map(i => enc.decode_locally(i).toString).
              zipWithIndex.map { case (v, i) => v -> (i + localOffset) }
          } else {
            val droppedBits = enc.bits.length - (numBits - 1)
            ("NULL" -> 0) +: (0 to enc.maxIdx).groupBy(_ >> droppedBits).toVector.sortBy(_._1).map { case (grp, idxes) =>
              val first = enc.decode_locally(idxes.min).toString
              val last = enc.decode_locally(idxes.max).toString
              first + " to " + last
            }.zipWithIndex.map { case (v, i) => v -> (i + localOffset) }
          }
      }
  }

  def values(numBits: Int) = decodePrefix(enc, numBits)
  def fullName(numBits: Int) = {
    val grp = if (numBits == bits.size) ""
    else "/" + (1 << (bits.size - numBits))
    name + grp
  }
  val bits = enc.allBits

  def selectedBits(numBits: Int) = bits.takeRight(numBits)
}

case class MyDimHierarchy(name: String, levels: IndexedSeq[MyDimLevel]) {
  lazy val flattenedLevels = {
    levels.flatMap { l =>
      val totalbits = l.bits.size
      if (l.enc.isInstanceOf[MergedMemColEncoder[_]])
        Vector(l.fullName(totalbits) -> (l, totalbits)) //only level corresponding to all bits
      else
        (1 to totalbits).map { nb => l.fullName(nb) -> (l, nb) } //exclude 0-bit level
    }
  }
  lazy val flattenedLevelsMap = flattenedLevels.toMap
}

class SudokubeServiceImpl(implicit mat: Materializer) extends SudokubeService {
  implicit val backend = CBackend.default
  implicit val ec = ExecutionContext.global
  def getDimHierarchy(dim: Dim2): Vector[MyDimHierarchy] = dim match {
    case BD2(rootname, children, cross) => if (cross) {
      children.map(c => getDimHierarchy(c)).reduce(_ ++ _)
    } else {
      //assuming all children are LD2 here
      val levels = children.flatMap {
        case LD2(name, encoder) if encoder.isInstanceOf[StaticDateCol]=>
          val date = encoder.asInstanceOf[StaticDateCol]
          val l2 = date.internalEncoders.map {
          case (iname, ienc) => MyDimLevel(name + "-" + iname, ienc)
          }
          l2
        case LD2(name, encoder) => Vector(MyDimLevel(name, encoder))
      }
      val hierarchy = MyDimHierarchy(rootname, levels)
      Vector(hierarchy)
    }
    case LD2(name, encoder) if encoder.isInstanceOf[StaticDateCol] =>
      val date = encoder.asInstanceOf[StaticDateCol]
      val levels = date.internalEncoders.map { case (iname, ienc) =>
        MyDimLevel(name + "-" + iname, ienc)
      }
      val hierarchy = MyDimHierarchy(name, levels)
      Vector(hierarchy)
    case LD2(name, encoder) =>
      val level = MyDimLevel(name, encoder)
      val hierarchy = MyDimHierarchy(name, Vector(level))
      Vector(hierarchy)
    case r@DynBD2() => r.children.values.map(c => getDimHierarchy(c)).reduce(_ ++ _)
  }
  def getCubeGenerator(cname: String, fullname: String = ""): AbstractCubeGenerator[_, _] = {
    print(s"cname = $cname")
    cname match {
      case "SSB-sf100" => new SSB(100)
      case "NYC" => new NYC()
      //case "WebshopSales" => new WebshopSales()
      case "WebshopSalesMulti" => new WebshopSalesMulti()
      case "WebShopDyn" => new WebShopDyn()
      case "TinyData" => new TinyDataStatic()
      case "TestDataDyn" => new TestTinyData()
      case "TinyDataDyn" => new TinyData()
      case s if s.startsWith("TransformedView") =>
        val otherCG = getCubeGenerator(s.drop("TransformedView".length)).asInstanceOf[CubeGenerator[_]]
        new TransformedViewCubeGenerator(otherCG, fullname)
    }
  }
  def cuboidToDimSplit(cub: Seq[Int], cols: Vector[LD2[_]]) = {
    val groups = cub.groupBy(b => cols.find(l => l.encoder.allBits.contains(b)).get)
    groups.map { case (ld, bs) =>
      ld.name -> ld.encoder.allBits.reverse.map(b => bs.contains(b))
    }
  }

  object MaterializeState {
    var cg: AbstractCubeGenerator[_, _] = null
    var baseCuboid: AbstractDataCube[_] = null
    var schema: Schema2 = null
    var columnMap: Map[String, ColEncoder[_]] = null
    val chosenCuboids = ArrayBuffer[IndexedSeq[Int]]()
    val shownCuboids = ArrayBuffer[IndexedSeq[Int]]()
    val shownCuboidsManualView = ArrayBuffer[(IndexedSeq[Int], Boolean)]()
  }

  object ExploreState {
    var dc: DataCube = null
    var cg: AbstractCubeGenerator[_, _] with DynamicCubeGenerator = null
    var sch: DynamicSchema2 = null
    var columnMap: Map[String, DynamicColEncoder[_]] = null
    val timeDimension = "RowId" //cg.timeDimension.getOrElse("RowId")
    var totalTimeBits = 5
    var exploreResult = 0 //0 -> No info, 1 -> ColIsAdded, 2 -> ColIsDeleted
    val sliceArray = collection.mutable.ArrayBuffer[Boolean]() //0 in this array refers to the leftmost, most significant bit in dimension
    var colNotNullBit = 0
    var timeRange = Array(0, 0, 0, 0)
  }

  object QueryState {
    var sch: Schema2 = null
    var hierarchy: Vector[MyDimHierarchy] = null
    var columnMap: Map[String, Map[String, (MyDimLevel, Int)]] = null

    val stats = collection.mutable.Map[String, Double]() withDefaultValue (0.0)

    var shownSliceValues: IndexedSeq[(String, Int, Boolean)] = null //value, original id, and isSelected
    var cubsFetched = 0
    var prepareCuboids = Seq[NewProjectionMetaData]()
    val filters = ArrayBuffer[(String, String, ArrayBuffer[(String, Int)])]()
    var filterCurrentDimArgs: (String, String) = null

    var isBatch = true
    var sortedQuery = IndexedSeq[Int]()
    var computeSortedIdx: ((Int, Int) => Int) = null
    var aggBits = Seq[Int]()
    var diceBits = Seq[Int]()
    var diceArgs = Seq[Seq[Int]]()
    var validXvalues = Seq[(String, Int)]()
    var validYvalues = Seq[(String, Int)]()
    var prepareNumCuboidsInpage = 0

    var cg: AbstractCubeGenerator[_, _] = null
    var agg: AggregationFunction = SUM
    var measures = IndexedSeq[String]()
    var dcs: IndexedSeq[DataCube] = Vector()
    var relevantDCs: IndexedSeq[DataCube] = Vector()
    var postProcessFn: Function1[IndexedSeq[Array[Double]], Array[Double]] = null
    var mergingViewTransformFn: Function1[Array[Double], Array[Double]] = null
    var solvers = IndexedSeq[Object]()
    var solverType: METHOD = NAIVE
  }


  /* Materialize */
  override def getBaseCuboids(in: Empty): Future[BaseCuboidResponse] = {
    Future.successful(BaseCuboidResponse(List("WebshopSalesMulti", "NYC", "SSB", "WebShopDyn", "TinyData", "TinyDataDyn")))
  }
  override def selectBaseCuboid(in: SelectBaseCuboidArgs): Future[SelectBaseCuboidResponse] = {
    import MaterializeState._
    val dsname = in.cuboid
    println("SelectBaseCuboid arg:" + in)
    Future {
      try {
        cg = getCubeGenerator(dsname)
        schema = cg.schemaInstance
        val dims = schema.columnVector.map { case LD2(name, enc) => CuboidDimension(name, enc.allBits.size) }
        columnMap = schema.columnVector.map { case LD2(name, enc) => name -> enc }.toMap
        baseCuboid = cg.loadBase()
        shownCuboids.clear()
        shownCuboidsManualView.clear()
        chosenCuboids.clear()
        val res = SelectBaseCuboidResponse(dims)
        println("\t response: " + res)
        res
      } catch {
        case ex: Exception => throw new GrpcServiceException(Status.INTERNAL.withCause(ex).withDescription(ex.toString))
      }
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
    try {
      val cubsInPage = in.rowsPerPage
      def filterCondition(cub: IndexedSeq[Int]) = {
        val cubSet = cub.toSet
        in.filters.map { f =>
          val allbits = columnMap(f.dimensionName).allBits
          val end = allbits.size - f.bitsFrom //reverse order
          val start = allbits.size - f.bitsTo - 1
          val filterBits = allbits.slice(start, end).toSet
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
    } catch {
      case ex: Exception => Future.failed(new GrpcServiceException(Status.INTERNAL.withCause(ex).withDescription(ex.toString)))
    }
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
      try {
        val cubsInPage = in.rowsPerPage
        val nbits = baseCuboid.index.n_bits
        val filterBits = in.filters.map { f =>
          val allbits = columnMap(f.dimensionName).allBits
          val end = allbits.size - f.bitsFrom //reverse order
          val start = allbits.size - f.bitsTo - 1
          allbits.slice(start, end)
        }.fold(Vector())(_ ++ _)
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
          val cubsK = bitsToPick.combinations(k).slice(requestedCubStart.toInt, binom.toInt).map(c => c ++ filterBits).toVector
          println(s"cuboids with ($k + ${filterBits.size}) bits = " + cubsK)
          availableCuboidsView ++= cubsK
          nextNumCuboids -= cubsK.size
          requestedCubStart = 0
          binom *= (n2 - k)
          binom /= (k + 1)
          k += 1
        }

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
      } catch {
        case ex: Exception => throw new GrpcServiceException(Status.INTERNAL.withCause(ex).withDescription(ex.toString))
      }
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
      cg.savePartial(ms, cg.inputname + "_" + in.cubeName)
      println("\t response: OK")
      Empty()
    }
  }

  /* Explore */
  override def getDataCubesForExplore(in: Empty): Future[GetCubesResponse] = {
    println("GetDataCubes Explore")
    try {
      val file = File("cubedata")
      val potentialCubes = file.toDirectory.dirs.map(_.name)
      val dynamicCubes = potentialCubes.filter { fn =>
        fn.startsWith("WebShopDyn_")
      } //only dynamic schema, without subcubes
      //Give WebshopDyn as first entry
      val response = GetCubesResponse(("WebShopDyn_base" +: dynamicCubes.toVector).distinct)
      Future.successful(response)
    } catch {
      case ex: Exception => Future.failed(new GrpcServiceException(Status.INTERNAL.withCause(ex).withDescription(ex.toString)))
    }
  }
  override def selectDataCubeForExplore(in: SelectDataCubeArgs): Future[SelectDataCubeForExploreResponse] = {
    import ExploreState._
    Future {
      try {
        val cgName = in.cube.split("_")(0)
        cg = getCubeGenerator(cgName).asInstanceOf[AbstractCubeGenerator[_, _] with DynamicCubeGenerator] //We will only load dynamic here
        dc = cg match {
          case mcg: MultiCubeGenerator[_] =>
            mcg.loadPartialOrBase(in.cube).asInstanceOf[MultiDataCube].dcs.head //we run only count queries. Assume count is first
          case tvcg: TransformedViewCubeGenerator[_] => tvcg.loadPartialOrBase(in.cube).asInstanceOf[DataCube]
          case scg: CubeGenerator[_] => scg.loadPartialOrBase(in.cube).asInstanceOf[DataCube]

        }
        sch = cg.schemaInstance.asInstanceOf[DynamicSchema2]
        columnMap = sch.columnVector.map { case LD2(name, enc) => name -> enc.asInstanceOf[DynamicColEncoder[_]] }.toMap
        totalTimeBits = columnMap(timeDimension).bits.size //exclude isNotNULL bit
        SelectDataCubeForExploreResponse(columnMap.keys.toVector)
      } catch {
        case ex: Exception => throw new GrpcServiceException(Status.INTERNAL.withCause(ex).withDescription(ex.toString))
      }
    }
  }
  override def isRenameQuery(in: IsRenamedQueryArgs): Future[IsRenamedQueryResponse] = {
    import ExploreState._
    Future {
      try {
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
      } catch {
        case ex: Exception => throw new GrpcServiceException(Status.INTERNAL.withCause(ex).withDescription(ex.toString))
      }
    }
  }

  def doRenameTimeQuery = {
    import ExploreState._
    Future {
      try {
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
      } catch {
        case ex: Exception => throw new GrpcServiceException(Status.INTERNAL.withCause(ex).withDescription(ex.toString))
      }
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
    Future {
      import ExploreState._
      val viewname = in.newCubeName
      val removedCols = in.cols.flatMap { c => Vector(c.dim1, c.dim2) }
      val newCols = in.cols.map { c =>
        LD2(c.newDim,
          new MergedMemColEncoder[String](
            columnMap(c.dim1).asInstanceOf[MemCol[String]],
            columnMap(c.dim2).asInstanceOf[MemCol[String]]))
      }
      val newRoot = BD2("ROOT", (columnMap -- removedCols).map { case (k, v) => LD2(k, v) }.toVector ++ newCols, true)
      val newsch = new TransformedSchema(newRoot)
      val cgName = "TransformedView" + dc.cubeName + "__" + viewname
      newsch.save(cgName)
      Empty()
    }
  }

  /** Query */
  override def getDataCubesForQuery(in: Empty): Future[GetCubesResponse] = {
    println("GetDataCubes Explore")
    val file = File("cubedata")
    val potentialCubes = file.toDirectory.dirs.map(_.name).map(_.split("--")(0)) //represent all multi data cubes as one
    //Give Webshop as first entry
    val response = GetCubesResponse(("WebshopSalesMulti_base" +: potentialCubes.toVector).distinct)
    Future.successful(response)
  }

  override def selectDataCubeForQuery(in: SelectDataCubeArgs): Future[SelectDataCubeForQueryResponse] = {
    import QueryState._
    Future {
      try {
        println("SelectDataCubeForQuery arg:" + in)
        val cgName = in.cube.split("_")(0)
        cg = getCubeGenerator(cgName, in.cube)
        dcs = cg match {
          case mcg: MultiCubeGenerator[_] =>
            val mdc = cg.loadPartialOrBase(in.cube).asInstanceOf[MultiDataCube]
            mdc.dcs.foreach {
              case dc: PartialDataCube => dc.loadPrimaryMoments(dc.basename)
              case dc: DataCube => dc.loadPrimaryMoments(dc.cubeName)
            }
            mdc.dcs
          case TransformedViewCubeGenerator(otherCG, vname) =>
            val otherDCName = vname.drop("TransformedView".length).split("__")(0)
            val dc = cg.loadPartialOrBase(otherDCName).asInstanceOf[DataCube]
            dc.loadPrimaryMoments(otherCG.baseName)
            Vector(dc)
          case scg: CubeGenerator[_] =>
            val dc = cg.loadPartialOrBase(in.cube).asInstanceOf[DataCube]
            dc.loadPrimaryMoments(scg.baseName)
            Vector(dc)

        }
        //filters.clear()

        sch = cg.schemaInstance
        sch.initBeforeDecode()
        hierarchy = getDimHierarchy(sch.root)
        columnMap = hierarchy.map { h => h.name -> h.flattenedLevelsMap }.toMap
        shownSliceValues = null
        filters.clear()
        filterCurrentDimArgs = null
        val dims = hierarchy.map { h => DimHierarchy(h.name, h.flattenedLevels.map(_._1)) }
        val cuboidDims = sch.columnVector.map { case LD2(name, encoder) => CuboidDimension(name, encoder.allBits.size) }
        val measureNames = cg.measure match {
          case multi: CompositeMeasure[_, _] => multi.allNames
          case s: Measure[_, _] => Vector(s.name)
        }
        val res = SelectDataCubeForQueryResponse(dims, cuboidDims, measureNames)
        println("\t response:" + res)
        res
      } catch {
        case ex: Exception => throw new GrpcServiceException(Status.INTERNAL.withCause(ex).withDescription(ex.toString))
      }
    }
  }
  override def getValuesForSlice(in: GetSliceValuesArgs): Future[GetSliceValueResponse] = {
    import QueryState._
    println("GetValuesForSlice arg: " + in)
    try {
      val dname = in.dimensionName
      val lname = in.dimensionLevel
      val (level, numBits) = columnMap(dname)(lname)
      val allValuesWithIndex = level.values(numBits).iterator
      val fileteredValues = allValuesWithIndex.filter(_._1.contains(in.searchText))
      val rowStart = in.requestedPageId * in.numRowsInPage
      val rowEnd = (in.requestedPageId + 1) * in.numRowsInPage
      val abOpt = filters.find(f => f._1 == dname && f._2 == lname).map(_._3)
      shownSliceValues = fileteredValues.slice(rowStart, rowEnd).toVector.map { case (v, i) => (v, i, abOpt.map(ab => ab.exists(_._2 == i)).getOrElse(false)) }
      filterCurrentDimArgs = (dname, lname)
      val response = GetSliceValueResponse(shownSliceValues.map(_._1), shownSliceValues.map(_._3))
      println("Set shown slice values " + shownSliceValues)
      println("\t response: " + response)
      Future.successful(response)
    } catch {
      case ex: Exception => Future.failed(new GrpcServiceException(Status.INTERNAL.withCause(ex).withDescription(ex.toString)))
    }
  }

  override def getFilters(in: Empty): Future[GetFiltersResponse] = {
    import QueryState._
    println("GetFilters")
    try {
      val response = GetFiltersResponse(filters.map { f => GetFiltersResponse.FilterDef(f._1, f._2, f._3.map(_._1).mkString(";")) })
      println("\t response : " + response)
      Future.successful(response)
    } catch {
      case ex: Exception => Future.failed(new GrpcServiceException(Status.INTERNAL.withCause(ex).withDescription(ex.toString)))
    }
  }

  override def setValuesForSlice(in: SetSliceValuesArgs): Future[Empty] = {
    import QueryState._
    println("SetValueForSlice arg: " + in)
    println("\t currentlyShownSliceValues = " + shownSliceValues)
    try {
      if (filterCurrentDimArgs != null) {
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
      }
      Future.successful(Empty())
    } catch {
      case ex: Exception => Future.failed(new GrpcServiceException(Status.INTERNAL.withCause(ex).withDescription(ex.toString)))
    }
  }

  override def deleteFilter(in: DeleteFilterArgs): Future[Empty] = {
    import QueryState._
    println("DeleteFilter arg: " + in)
    try {
      filters.remove(in.index)
      println("\t response: OK")
      Future.successful(Empty())
    } catch {
      case ex: Exception => Future.failed(new GrpcServiceException(Status.INTERNAL.withCause(ex).withDescription(ex.toString)))
    }
  }

  def runQuery(): Future[QueryResponse] = {
    import QueryResponse._
    import QueryState._

    Future {
      try {
        if (isBatch) {
          (relevantDCs zip solvers).foreach { case (dc, solver) =>
            solverType match {
              case NAIVE => Profiler("Fetch") {
                val result = dc.fetch2[Double](prepareCuboids)
                val naivesolver = solver.asInstanceOf[NaiveSolver]
                naivesolver.add(result)

              }
              case LPP =>
                import core.solver.RationalTools._
                val lpSolver = solver.asInstanceOf[SliceSparseSolver[Rational]]
                val allFetched = Profiler("Fetch") { prepareCuboids.map { pm => pm.queryIntersection -> dc.fetch2[Rational](List(pm)) } }
                Profiler("Solve") { allFetched.foreach { case (bits, array) => lpSolver.add2(List(bits), array) } }
              case MOMENT =>
                val momentSolver = solver.asInstanceOf[CoMoment5SolverDouble]
                val allFetched = Profiler("Fetch") { prepareCuboids.map { pm => pm.queryIntersection -> dc.fetch2[Double](List(pm)) } }
                Profiler("Solve") { allFetched.foreach { case (bits, array) => momentSolver.add(bits, array) } }
              case IPF =>
                val ipfSolver = solver.asInstanceOf[NewVanillaIPFSolver]
                val allFetched = Profiler("Fetch") { prepareCuboids.map { pm => pm.queryIntersection -> dc.fetch2[Double](List(pm)) } }
                Profiler("Solve") { allFetched.foreach { case (bits, array) => ipfSolver.add(bits, array) } }
            }
          }
          cubsFetched += prepareCuboids.size
        }
        else {
          val nextCuboidToFetch = prepareCuboids(cubsFetched)
          cubsFetched += 1
          (relevantDCs zip solvers).foreach { case (dc, solver) =>
            solverType match {
              case NAIVE => Profiler("Fetch") {
                val result = dc.fetch2[Double](prepareCuboids)
                val naivesolver = solver.asInstanceOf[NaiveSolver]
                naivesolver.add(result)
              }
              case LPP =>
                val lpSolver = solver.asInstanceOf[SliceSparseSolver[Rational]]
                import core.solver.RationalTools._
                val fetchedData = Profiler("Fetch") { dc.fetch2[Rational](List(nextCuboidToFetch)) }
                Profiler("Solve") { lpSolver.add2(List(nextCuboidToFetch.queryIntersection), fetchedData) }
              case MOMENT =>
                val momentSolver = solver.asInstanceOf[CoMoment5SolverDouble]
                val fetchedData = Profiler("Fetch") { dc.fetch2[Double](List(nextCuboidToFetch)) }
                Profiler("Solve") { momentSolver.add(nextCuboidToFetch.queryIntersection, fetchedData) }
              case IPF =>
                val ipfSolver = solver.asInstanceOf[NewVanillaIPFSolver]
                val fetchedData = Profiler("Fetch") { dc.fetch2[Double](List(nextCuboidToFetch)) }
                Profiler("Solve") { ipfSolver.add(nextCuboidToFetch.queryIntersection, fetchedData) }
            }
          }
        }
        val (series, dof) = if (solverType != LPP) {
          val (sortedResults, dof) = Profiler("Solve") {
            solverType match {
              case NAIVE =>
                solvers.map { solver =>
                  solver.asInstanceOf[NaiveSolver].solution
                } -> 0
              case MOMENT =>
                solvers.map { solver =>
                  val momentSolver = solver.asInstanceOf[CoMoment5SolverDouble]
                  momentSolver.fillMissing()
                  momentSolver.solve()
                } -> solvers.head.asInstanceOf[CoMoment5SolverDouble].dof
              case IPF =>
                solvers.map { solver =>
                  val ipfSolver = solver.asInstanceOf[NewVanillaIPFSolver]
                  ipfSolver.solve()
                } -> solvers.head.asInstanceOf[NewVanillaIPFSolver].dof
            }
          }
          val sortedRes = postProcessFn(sortedResults)
          val viewTransformedRes = mergingViewTransformFn(sortedRes)
          //println("SortedRes = " + sortedRes.mkString(" "))
          //println("ViewTransformedRes =" + viewTransformedRes.mkString(" "))
          //println("DiceBits = " + diceBits)
          //println("DiceArgs = " + diceArgs)
          val diceRes = Util.dice(viewTransformedRes, aggBits, diceBits, diceArgs)
          //println("diceRes = " + diceRes.mkString(" "))
          val series = validYvalues.reverse.map { case (ylabel, yid) => //we reverse the order of series
            SeriesData(ylabel, validXvalues.map { case (xlabel, xid) =>
              val si = computeSortedIdx(xid, yid)
              XYPoint(xlabel, diceRes(si).toFloat)
            })
          }
          (series, dof)
        } else {
          assert(solvers.size == 1)
          val lpSolver = solvers.head.asInstanceOf[SliceSparseSolver[Rational]]
          val bounds = Profiler("Solve") {
            lpSolver.gauss(lpSolver.det_vars)
            lpSolver.compute_bounds
            lpSolver.bounds.toArray.map(x => (x.lb.get.toDouble, x.ub.get.toDouble))
          }
          //skip postProcessFn , we only have single measure
          //skip view transformation, not supported for LP
          val lower = bounds.map(_._1)
          val upper = bounds.map(_._2)
          val diceResLower = Util.dice(lower, aggBits, diceBits, diceArgs)
          val diceResUpper = Util.dice(upper, aggBits, diceBits, diceArgs)
          val diceResMiddle = (diceResLower zip diceResUpper).map { case (l, u) => (l + u) / 2 }
          val series = validYvalues.reverse.flatMap { case (ylabel, yid) => //we reverse the order of series
            Vector(
              SeriesData(ylabel + "-mid", validXvalues.map { case (xlabel, xid) =>
                val si = computeSortedIdx(xid, yid)
                XYPoint(xlabel, diceResMiddle(si).toFloat)
              }),
              SeriesData(ylabel + "-low", validXvalues.map { case (xlabel, xid) =>
                val si = computeSortedIdx(xid, yid)
                XYPoint(xlabel, diceResLower(si).toFloat)
              }),
              SeriesData(ylabel + "-high", validXvalues.map { case (xlabel, xid) =>
                val si = computeSortedIdx(xid, yid)
                XYPoint(xlabel, diceResUpper(si).toFloat)
              })
            )
          }
          (series, lpSolver.df)
        }
        println("AllSeries")
        series.sortBy(_.seriesName).map { s => s.seriesName + ":" + s.data.map { xy => xy.x -> xy.y }.mkString(" ") }.foreach(println)
        stats("PrepareTime(ms)") += Profiler.getDurationMicro("Prepare") / 1000.0
        stats("FetchTime(ms)") += Profiler.getDurationMicro("Fetch") / 1000.0
        stats("SolveTime(ms)") += Profiler.getDurationMicro("Solve") / 1000.0
        stats("DOF") = dof


        val statsToShow = stats.map(p => new QueryStatistic(p._1, p._2.toString)).toSeq
        val fetchedCuboidIDwithinPage = (cubsFetched - 1) % prepareNumCuboidsInpage
        val fetchedCuboidPageNum = (cubsFetched - 1) / prepareNumCuboidsInpage

        val start = fetchedCuboidPageNum * prepareNumCuboidsInpage
        val end = (fetchedCuboidPageNum + 1) * prepareNumCuboidsInpage
        val cuboidsToDisplay = prepareCuboids.slice(start, end).map { pm =>
          val cub = BitUtils.IntToSet(pm.queryIntersection).map { i => sortedQuery(i) }
          val dims = cuboidToDimSplit(cub, sch.columnVector).map { case (k, v) => DimensionBits(k, v) }.toSeq
          CuboidDef(dims)
        }


        val isComplete = cubsFetched == prepareCuboids.size
        val response = QueryResponse(fetchedCuboidPageNum, cuboidsToDisplay, fetchedCuboidIDwithinPage, isComplete, series, statsToShow)
        response
      } catch {
        case ex: Exception =>
          ex.printStackTrace(System.err)
          throw new GrpcServiceException(Status.INTERNAL.withCause(ex).withDescription(ex.toString))
      }
    }
  }
  def extractValidLabelsAndIndexesForDims(dims: Seq[QueryArgs.DimensionDef]) = {
    import QueryState._
    println("Processing labels for " + dims)
    val isSoloDim = dims.size == 1
    val foldResult = if (dims.isEmpty)
      Vector("(all)" -> 0) -> 0
    else
      dims.map { d =>
        val (level, numBits) = columnMap(d.dimensionName)(d.dimensionLevel)
        val validValues = level.values(numBits)
        (d.dimensionLevel, validValues, numBits)
      }.foldLeft(IndexedSeq("" -> 0), 0) { case ((accvv, accbits), (dname, curvv, curbits)) =>
        curvv.flatMap { case (curv, curi) =>
          accvv.map { case (accv, acci) =>
            //put curi as higherorder bits infront of acci
            //Put curv infront of accv
            val newi = (curi << accbits) + acci
            val newv = (if (isSoloDim) curv else s"$dname=$curv ") + accv
            (newv, newi)
          }
        } -> (accbits + curbits)
      } //labels and indexes of valid rows within (0 .. numEntries)
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
    prepareNumCuboidsInpage = in.preparedCuboidsPerPage
    agg = in.aggregation match {
      case "SUM" => SUM
      case "AVG" => AVERAGE
      case "COUNT" => COUNT
      case "VAR" => VARIANCE
      case "COR" => CORRELATION
      case "REG" => REGRESSION
    }
    solverType = in.solver match {
      case "Naive" => NAIVE
      case "Linear Programming" => LPP
      case "Moment" => MOMENT
      case "Graphical Model" => IPF
    }
    if (solverType == LPP && agg != SUM) {
      Future.failed(new GrpcServiceException(Status.UNIMPLEMENTED.withDescription(s"$agg not supported with Linear Programming solver")))
    } else if (solverType == LPP && cg.isInstanceOf[TransformedViewCubeGenerator[_]]) {
      Future.failed(new GrpcServiceException(Status.UNIMPLEMENTED.withDescription(s"Linear Programming solver is not supported for views")))
    } else {
      try {
        measures = Vector(in.measure, in.measure2)

        val defaultPostProcess = ((data: IndexedSeq[Array[Double]]) => data.head)
        val (idxes, postProcess) = cg match {
          case mcg: MultiCubeGenerator[_] =>
            val r = mcg.measure.indexAndPostProcessQuery(agg, measures)
            r._1 -> r._2
          case scg: CubeGenerator[_] =>
            if (agg == SUM && in.measure == scg.measure.name)
              Vector(0) -> defaultPostProcess
            else if (agg == COUNT && in.measure.isInstanceOf[CountMeasure[_]]) {
              Vector(0) -> defaultPostProcess
            }
            else
              throw new GrpcServiceException(Status.UNIMPLEMENTED.withDescription(s"Aggregation $agg on ${measures.mkString(",")} not supported"))
        }
        relevantDCs = idxes.map { i => dcs(i) }
        postProcessFn = postProcess

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
        diceArgs = diceBitsAndArgs.map(_._2).foldLeft(Vector(Vector[Int]())) { case (accvss, curvss) =>
          accvss.flatMap { accvs =>
            curvss.map { curvs =>
              accvs ++ curvs
            }
          }
        }
        val aggQuery = (xbits ++ ybits).toVector
        val fullQuery = (aggQuery ++ zbits).toVector
        sortedQuery = fullQuery.sorted.distinct
        println(s"aggQuery $aggQuery, fullQuery = $fullQuery, sorted = $sortedQuery")
        aggBits = aggQuery.map { b => sortedQuery.indexOf(b) }
        diceBits = zbits.map { b => sortedQuery.indexOf(b) } //Dice is done on the sorted result before permuting
        val permf = Util.permute_unsortedIdx_to_sortedIdx(aggQuery) //dice is already done, so only aggdims remain
        computeSortedIdx = (xid: Int, yid: Int) => {
          val ui = yid * xtotal + xid
          val si = permf(ui)
          println(s"x=$xid y=$yid ui=$ui si=$si")
          si
        }

        isBatch = in.isBatchMode

        val idFn = (r: Array[Double]) => r
        if (cg.isInstanceOf[TransformedViewCubeGenerator[_]]) {
          val mergedCols = cg.schemaInstance.columnVector.map(_.encoder).filter(_.isInstanceOf[MergedMemColEncoder[_]]).map(_.asInstanceOf[MergedMemColEncoder[_]])
          mergingViewTransformFn = mergedCols.foldLeft(idFn) { case (acc, cur) => acc andThen cur.getTransformFunction(sortedQuery) }
        } else {
          mergingViewTransformFn = idFn
        }
        prepareCuboids = Profiler("Prepare") {
          if (solverType == NAIVE) {
            dcs.head.index.prepareNaive(sortedQuery)
          }
          else if (isBatch) {
            dcs.head.index.prepareBatch(sortedQuery)
          } else
            dcs.head.index.prepareOnline(sortedQuery, 2)
        }

        solverType match {
          case NAIVE => solvers = relevantDCs.map { dc => new NaiveSolver() }
          case LPP =>
            import core.solver.RationalTools._
            solvers = Profiler("Solve") {
              relevantDCs.map { dc =>
                val b1 = SolverTools.mk_all_non_neg[Rational](1 << sortedQuery.size)
                new SliceSparseSolver[Rational](sortedQuery.length, b1, Nil, Nil)
              }
            }
          case MOMENT =>
            val pms = Profiler("Prepare") {
              relevantDCs.map { dc => SolverTools.preparePrimaryMomentsForQuery[Double](sortedQuery, dc.primaryMoments) }
            }
            solvers = Profiler("Solve") {
              pms.map { pm => new CoMoment5SolverDouble(sortedQuery.size, isBatch, null, pm) }
            }
          case IPF =>
            solvers = Profiler("Solve") {
              relevantDCs.map { dc => new NewVanillaIPFSolver(sortedQuery.size) }
            }
        }
      } catch {
        case ex: Exception =>
          ex.printStackTrace(System.err)
          Future.failed(new GrpcServiceException(Status.INTERNAL.withCause(ex).withDescription(ex.toString)))
      }
      runQuery()
    }
  }

  override def continueQuery(in: Empty): Future[QueryResponse] = {
    runQuery()
  }

  override def getPreparedCuboids(in: GetPreparedCuboidsArgs): Future[GetPreparedCuboidsResponse] = {
    import QueryState._
    try {
      val start = in.numRowsInPage * in.requestedPageId
      val end = in.numRowsInPage * (in.requestedPageId + 1)
      val cuboidsToDisplay = prepareCuboids.slice(start, end).map { pm =>
        val cub = BitUtils.IntToSet(pm.queryIntersection).map { i => sortedQuery(i) }
        val dims = cuboidToDimSplit(cub, sch.columnVector).map { case (k, v) => DimensionBits(k, v) }.toSeq
        CuboidDef(dims)
      }
      val response = GetPreparedCuboidsResponse(cuboidsToDisplay)
      Future.successful(response)
    } catch {
      case ex: Exception => Future.failed(new GrpcServiceException(Status.INTERNAL.withCause(ex).withDescription(ex.toString)))
    }
  }
}
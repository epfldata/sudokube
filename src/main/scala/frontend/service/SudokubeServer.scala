package frontend.service

import akka.actor.ActorSystem
import akka.grpc.scaladsl.WebHandler
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.Materializer
import backend.CBackend
import com.typesafe.config.ConfigFactory
import core.materialization.{PresetMaterializationStrategy, RandomizedMaterializationStrategy, SchemaBasedMaterializationStrategy}
import core.{DataCube, PartialDataCube}
import frontend.generators._
import frontend.schema.encoders.{ColEncoder, DynamicColEncoder}
import frontend.schema.{DynamicSchema2, LD2, Schema2}
import frontend.service.GetRenameTimeResponse.ResultRow
import util.Util

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.io.File
import scala.util.Random
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


class SudokubeServiceImpl(implicit mat: Materializer) extends SudokubeService {
  implicit val backend = CBackend.default
  implicit val ec = ExecutionContext.global
  def getCubeGenerator(cname: String) = {
    cname match {
      case "SSB" => new SSB(100)
      case "NYC" => new NYC()
      case "WebShop" => new WebshopSales()
      case "WebShopDyn" => new WebShopDyn()
      case "TinyData" => new TinyData()
    }
  }
  def cuboidToDimSplit(cub: IndexedSeq[Int], cols: Vector[LD2[_]]) = {
    val groups = cub.groupBy(b => cols.find(l => l.encoder.bits.contains(b)).get)
    groups.map { case (ld, bs) =>
      ld.name -> ld.encoder.bits.reverse.map(b => bs.contains(b)) }
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
    var exploreResult = 0  //0 -> No info, 1 -> ColIsAdded, 2 -> ColIsDeleted
    val sliceArray = collection.mutable.ArrayBuffer[Boolean]() //0 in this array refers to the leftmost, most significant bit in dimension
    var colNotNullBit = 0
    var timeRange = Array(0, 0, 0 ,0)
  }

  object QueryState {
    val stats = collection.mutable.Map[String, String]()
    var numPrepared = 0
    var cubsFetched = 0
    var prepareCuboids = Seq[CuboidDef]()
    var shownCuboids = Seq[CuboidDef]()
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
      val dims = schema.columnVector.map { case LD2(name, enc) => Dimension(name, enc.bits.size) }
      columnMap = schema.columnVector.map { case LD2(name, enc) => name -> enc }.toMap
      baseCuboid = cg.loadBase()
      shownCuboids.clear()
      shownCuboidsManualView.clear()
      chosenCuboids.clear()
      println("Base cuboid" + baseCuboid.cubeName +  "  loaded")
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
    println("\t response: OK" )
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
      if(k < n2)  //do not pick all bits
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
      println("\t response: OK" )
      Empty()
    }
  }

  /* Explore */
  override def getDataCubes(in: Empty): Future[GetCubesResponse] = {
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
      dc = if(in.cube.endsWith("_base")){
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
      val isRenamed = (result(0) == 0) && ( result(3) == 0)
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
        val sliceArgs = sliceArray.zipWithIndex.map{case (sv, i) => (fullquery.size-1-i) -> (if(sv) 1 else 0)}
        val slicedRes = Util.slice(unsortedRes, sliceArgs)
        println("RenameTime queryRes = " + slicedRes.toVector)
        val row0 = ResultRow(timeRange(0) + "-" + timeRange(1), slicedRes(0), slicedRes(1))
        val row1 = ResultRow(timeRange(2) + "-" + timeRange(3), slicedRes(2), slicedRes(3))
        val zeroIndices = slicedRes.indices.filter(i => slicedRes(i) == 0).toSet
        var isContinue = true
        assert(slicedRes.size == 4)
        val currentSlice = sliceArray.toVector
        val currentTimeRange = timeRange.clone()
        if(zeroIndices.isEmpty) {
          isContinue = false
        }
        else if(Set(0,2).subsetOf(zeroIndices)) isContinue = false  //no rename
         //(0,1) and (2,3) both zero not possible. (1,3) zero means all null
        else if(Set(0,3).subsetOf(zeroIndices)) {
          isContinue = false
          exploreResult = 2 //column deleted

        } else if (Set(1, 2).subsetOf(zeroIndices)) {
          isContinue = false
          exploreResult = 1 //column added
        } else if (zeroIndices.contains(0)) {
          exploreResult = 2 //col deleted somewhere in 1 slice
          sliceArray += true
          timeRange(0) = timeRange(2)
          timeRange(1) = (timeRange(3)+timeRange(0)-1)/2
          timeRange(2) = (timeRange(3)+timeRange(0)+1)/2
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
        val string = if(!isContinue) {
           exploreResult match {
            case 0 => "No renaming could be detected within the available resolution"
            case 1 => s"Column possibly added between ${currentTimeRange(1)} and ${currentTimeRange(2)}"
            case 2 => s"Column possibly deleted between ${currentTimeRange(1)} and ${currentTimeRange(2)}"
          }
        } else {
          if(sliceArray.last) "Slicing to upper range" else "Slicing to lower range"
        }
        println("RenameTime currentTimeRange = " + currentTimeRange.toVector + "   nextTimeRange = " + timeRange.toVector)
        println(string)
        GetRenameTimeResponse(totalTimeBits, !isContinue,  currentSlice, Vector(row0, row1), string)
      }
  }
  override def startRenameTimeQuery(in: GetRenameTimeArgs): Future[GetRenameTimeResponse] = {
    import ExploreState._
    exploreResult = 0
    sliceArray.clear()
    timeRange(0) = 0
    timeRange(3) = (1 << totalTimeBits) - 1
    timeRange(1) = (timeRange(0) + timeRange(3) -1)/2
    timeRange(1) = (timeRange(0) + timeRange(3) +1)/2
    colNotNullBit =  columnMap(in.dimensionName).isNotNullBit
    doRenameTimeQuery
  }
  override def continueRenameTimeQuery(in: Empty): Future[GetRenameTimeResponse] = {
    doRenameTimeQuery
  }
  override def transformCube(in: TransformDimensionsArgs): Future[Empty] = {
    Future.successful(Empty())
  }
  /** Query */
  override def selectDataCubeForQuery(in: SelectDataCubeArgs): Future[SelectDataCubeForQueryResponse] = {
    val dims = (1 to 10).map { i =>
      val dim = "Dim" + i
      val numBits = 6
      val levels = (1 to numBits).map { j => dim + "L" + j }
      SelectDataCubeForQueryResponse.DimHierarchy(dim, numBits, levels)
    }
    val response = SelectDataCubeForQueryResponse(dims)
    Future.successful(response)
  }
  override def getValuesForSlice(in: GetSliceValuesArgs): Future[GetSliceValueResponse] = {
    val values = (1 to 100).map(i => "V" + i)
    val shownValues = values.filter(_.contains(in.searchText)).take(in.numRowsInPage)
    val response = GetSliceValueResponse(shownValues)
    Future.successful(response)
  }

  // TODO: Implement something to get and post filters

  override def getFilters(in: Empty): Future[GetFiltersResponse] = ???

  override def setValuesForSlice(in: SetSliceValuesArgs): Future[Empty] = ???

  override def startQuery(in: QueryArgs): Future[QueryResponse] = {
    import QueryResponse._
    import QueryState._
    stats.clear()
    cubsFetched = 1
    numPrepared = Random.nextInt(100)
    prepareCuboids = (0 until numPrepared).map { j =>
      val dims = (1 to 10).map { i =>
        val total = 5
        val numBits = Random.nextInt(total + 1)
        val selectedBits = Util.collect_n(numBits, () => Random.nextInt(total)).toSet
        val bitsValues = bitsToBools(total, selectedBits)
        DimensionBits("Dim" + i, bitsValues)
      }
      CuboidDef(dims)
    }

    stats += ("PrepareTime" -> cubsFetched.toString)
    stats += ("FetchTime" -> (2 * cubsFetched).toString)
    stats += ("SolveTime" -> (3 * cubsFetched).toString)
    stats += ("Error" -> (0.4 - cubsFetched / 10.0).toString)

    shownCuboids = prepareCuboids.take(in.preparedCuboidsPerPage)
    val series = collection.mutable.ArrayBuffer[SeriesData]()
    series += SeriesData("Linear", (1 to 10).map { i => XYPoint("P" + i, i + cubsFetched) })
    series += SeriesData("Quadratic", (1 to 10).map { i => XYPoint("P" + i, math.pow(i + cubsFetched / 10.0, 2).toFloat) })
    series += SeriesData("Log", (1 to 10).map { i => XYPoint("P" + i, math.log(i + cubsFetched).toFloat) })
    val response = QueryResponse(0, shownCuboids, 0, series) // TODO?: Convert stats?
    Future.successful(response)
  }
  override def continueQuery(in: Empty): Future[QueryResponse] = {

    import QueryResponse._
    import QueryState._
    stats.clear()
    cubsFetched += 1
    stats += ("PrepareTime" -> cubsFetched.toString)
    stats += ("FetchTime" -> (2 * cubsFetched).toString)
    stats += ("SolveTime" -> (3 * cubsFetched).toString)
    stats += ("Error" -> (0.4 - cubsFetched / 10.0).toString)


    val series = collection.mutable.ArrayBuffer[SeriesData]()
    series += SeriesData("Linear", (1 to 10).map { i => XYPoint("P" + i, i + cubsFetched) })
    series += SeriesData("Quadratic", (1 to 10).map { i => XYPoint("P" + i, math.pow(i + cubsFetched / 10.0, 2).toFloat) })
    series += SeriesData("Log", (1 to 10).map { i => XYPoint("P" + i, math.log(i + cubsFetched).toFloat) })
    val response = QueryResponse(0, shownCuboids, 0, series) // TODO?: Convert stats?
    Future.successful(response)
  }

  // TODO: Implement something
  override def getPreparedCuboids(in: GetPreparedCuboidsArgs): Future[GetPreparedCuboidsResponse] = ???
}
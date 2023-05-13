package frontend.service

import akka.actor.ActorSystem
import akka.grpc.scaladsl.WebHandler
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.Materializer
import backend.CBackend
import com.typesafe.config.ConfigFactory
import combinatorics.Combinatorics
import core.{DataCube, PartialDataCube}
import core.materialization.{Base2MaterializationStrategy, PresetMaterializationStrategy, RandomizedMaterializationStrategy, SchemaBasedMaterializationStrategy}
import frontend.generators._
import frontend.schema.encoders.ColEncoder
import frontend.schema.{DynamicSchema, LD2, StaticSchema2}
import util.Util

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
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
      case "SSB" => new SSB(100) -> true
      case "NYC" => new NYC() -> true
      case "WebShop" => new WebshopSales() -> true
    }
  }
  def cuboidToDimSplit(cub: IndexedSeq[Int], cols: Vector[LD2[_]]) = {
    val groups = cub.groupBy(b => cols.find(l => l.encoder.bits.contains(b)).get)
    groups.map { case (ld, bs) => ld.name -> ld.encoder.bits.reverse.map(b => bs.contains(b)) }
  }

  object MaterializeState {
    var isStaticSchema = false
    var cg: CubeGenerator = null
    var baseCuboid: DataCube = null
    //TODO: Merge into common parent
    var staticSchema: StaticSchema2 = null
    var columnMap: Map[String, ColEncoder[_]] = null
    val chosenCuboids = ArrayBuffer[IndexedSeq[Int]]()
    val shownCuboids = ArrayBuffer[IndexedSeq[Int]]()
    val shownCuboidsManualView = ArrayBuffer[(IndexedSeq[Int], Boolean)]()
  }

  object ExploreState {
    var timeBits = 1
    var totalTimeBits = 5
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
    Future.successful(BaseCuboidResponse(List("WebShop", "NYC", "SSB")))
  }
  override def selectBaseCuboid(in: SelectBaseCuboidArgs): Future[SelectBaseCuboidResponse] = {
    import MaterializeState._
    import SelectBaseCuboidResponse._
    val dsname = in.cuboid
    println("SelectBaseCuboid " + in)
    Future {
      val cgstatic = getCubeGenerator(dsname)
      cg = cgstatic._1
      isStaticSchema = cgstatic._2
      staticSchema = cg.schemaInstance.asInstanceOf[StaticSchema2]
      val dims = staticSchema.columnVector.map { case LD2(name, enc) => Dimension(name, enc.bits.size) }
      columnMap = staticSchema.columnVector.map { case LD2(name, enc) => name -> enc }.toMap
      baseCuboid = cg.loadBase()
      println("Base cuboid loaded")
      SelectBaseCuboidResponse(dims)
    }
  }

  override def selectMaterializationStrategy(in: SelectMaterializationStrategyArgs): Future[Empty] = {
    import MaterializeState._
    val args = in.args.map(_.toInt).toVector
    val ms = in.name match {
      case "Randomized" => new RandomizedMaterializationStrategy(baseCuboid.index.n_bits, args(0), args(1))
      case "Prefix" => new SchemaBasedMaterializationStrategy(staticSchema, args(0), args(1))
    }
    chosenCuboids.clear()
    chosenCuboids ++= ms.projections
    Future.successful(Empty())
  }

  override def getChosenCuboids(in: GetCuboidsArgs): Future[GetChosenCuboidsResponse] = {
    import MaterializeState._
    val cubsInPage = in.rowsPerPage
    def filterCondition(cub: IndexedSeq[Int]) = {
      val cubSet = cub.toSet
      in.filters.map { f =>
        val filterBits = columnMap(f.dimensionName).bits.slice(f.bitsFrom, f.bitsTo).toSet
        filterBits.subsetOf(cubSet)
      }.fold(true)(_ && _)
    }
    val filteredCuboids = chosenCuboids.filter(filterCondition(_))
    val numCuboids = filteredCuboids.size
    val requestedCuboidsEnd = ((in.requestedPageId + 1) * cubsInPage) min numCuboids //exclusive
    val requestedCuboidsStart = (requestedCuboidsEnd - cubsInPage) max 0
    shownCuboids.clear()
    shownCuboids ++= filteredCuboids.slice(requestedCuboidsStart, requestedCuboidsEnd)
    val cubs = shownCuboids.map { cub =>
      val dims = cuboidToDimSplit(cub, staticSchema.columnVector).map { case (k, v) => DimensionBits(k, v) }.toSeq
      CuboidDef(dims)
    }
    Future.successful(GetChosenCuboidsResponse(cubs))
  }

  override def deleteChosenCuboid(in: DeleteSelectedCuboidArgs): Future[Empty] = {
    import MaterializeState._
    val id = in.cuboidIdWithinPage
    val deletedCuboid = shownCuboids(id)
    shownCuboids -= deletedCuboid
    chosenCuboids -= deletedCuboid
    Future.successful(Empty())
  }
  override def getAvailableCuboids(in: GetCuboidsArgs): Future[GetAvailableCuboidsResponse] = {
    import MaterializeState._
    println("getAvailableCuboids " + in)
    Future {
      val cubsInPage = in.rowsPerPage
      val nbits = baseCuboid.index.n_bits
      val filterBits = in.filters.map { f => columnMap(f.dimensionName).bits.slice(f.bitsFrom, f.bitsTo) }.reduce(_ ++ _)
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
      availableCuboidsView ++= bitsToPick.combinations(k).slice(requestedCubStart.toInt, (requestedCubStart + nextNumCuboids).toInt).map(c => c ++ filterBits)
      println("Filter Bits = " + filterBits)
      println("Available Cuboids = ")
      availableCuboidsView.foreach(println)
      shownCuboidsManualView.clear()
      shownCuboidsManualView ++= availableCuboidsView.map { cub =>
        val isChosen = chosenCuboids.contains(cub)
        cub -> isChosen
      }
      val cubs = shownCuboidsManualView.map { case (cub, isC) =>
        val dims = cuboidToDimSplit(cub, staticSchema.columnVector).map { case (k, v) => DimensionBits(k, v) }.toSeq
        GetAvailableCuboidsResponse.ManualSelectionCuboidDef(dims, isC)
      }
      GetAvailableCuboidsResponse(cubs)
    }
  }
  override def manuallyUpdateCuboids(in: ManuallyUpdateCuboidsArgs): Future[Empty] = {
    import MaterializeState._
    in.isChosen.zip(shownCuboidsManualView).foreach { case (afterChosen, (cub, beforeChosen)) =>
      if (beforeChosen && !afterChosen)
        chosenCuboids -= cub
      else if (!beforeChosen && afterChosen)
        chosenCuboids += cub
    }
    Future.successful(Empty())
  }
  override def materializeCuboids(in: MaterializeArgs): Future[Empty] = {
    import MaterializeState._
    Future {
      val ms = PresetMaterializationStrategy(baseCuboid.index.n_bits, chosenCuboids)
      val dc = new PartialDataCube(in.cubeName, baseCuboid.cubeName)
      dc.buildPartial(ms)
      dc.save()
      Empty()
    }
  }

  /* Explore */
  override def getDataCubes(in: Empty): Future[GetCubesResponse] = {
    val response = GetCubesResponse((1 to 10).map(i => "DataCube" + i))
    Future.successful(response)
  }
  override def selectDataCubeForExplore(in: SelectDataCubeArgs): Future[SelectDataCubeForExploreResponse] = {
    val dims = (1 to 10).map(i => "Dim" + i)
    val response = SelectDataCubeForExploreResponse(dims)
    Future.successful(response)
  }
  override def isRenameQuery(in: IsRenamedQueryArgs): Future[IsRenamedQueryResponse] = {
    val result = Array(0, 10, 20, 0)
    val isRenamed = Random.nextBoolean()
    val response = IsRenamedQueryResponse(result, isRenamed)
    Future.successful(response)
  }
  override def startRenameTimeQuery(in: GetRenameTimeArgs): Future[GetRenameTimeResponse] = {
    import ExploreState._
    timeBits = 1
    val isComplete = timeBits >= totalTimeBits
    val bits = (0 until timeBits).map { i => Random.nextBoolean() }
    val result = Array(GetRenameTimeResponse.ResultRow("0", 0, 10), GetRenameTimeResponse.ResultRow("1", 20, 0))
    val string = if (isComplete) "Column Changed between X and Y" else ""
    val response = GetRenameTimeResponse(timeBits, isComplete, bits, result, string)
    Future.successful(response)
  }
  override def continueRenameTimeQuery(in: Empty): Future[GetRenameTimeResponse] = {
    import ExploreState._
    timeBits += 1
    val isComplete = timeBits >= totalTimeBits
    val bits = (0 until timeBits).map { i => Random.nextBoolean() }
    val result = Array(GetRenameTimeResponse.ResultRow("0", 0, 10), GetRenameTimeResponse.ResultRow("1", 20, 0))
    val string = if (isComplete) "Column Changed between X and Y" else ""
    val response = GetRenameTimeResponse(timeBits, isComplete, bits, result, string)
    Future.successful(response)
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
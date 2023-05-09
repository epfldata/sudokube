package frontend.service

import akka.actor.ActorSystem
import akka.grpc.scaladsl.WebHandler
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.Materializer
import com.typesafe.config.ConfigFactory
import util.Util

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
  object MaterializeState {

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
  /* Materialoze */
  override def getBaseCuboids(in: Empty): Future[BaseCuboidResponse] = {
    Future.successful(BaseCuboidResponse(List("NYC", "SSB", "AirlineDelay")))
  }
  override def selectBaseCuboid(in: SelectBaseCuboidArgs): Future[SelectBaseCuboidResponse] = {
    val dims = (1 to 10).map(i => SelectBaseCuboidResponse.Dimension("Dim" + i, 6))
    val response = SelectBaseCuboidResponse(dims)
    Future.successful(response)
  }

  override def selectMaterializationStrategy(in: SelectMaterializationStrategyArgs): Future[Empty] = {
    //TODO
    Future.successful(Empty())
  }
  override def getChosenCuboids(in: GetCuboidsArgs): Future[GetChosenCuboidsResponse] = {
    val numCuboids = Random.nextInt(100)
    val cubsInPage = in.rowsPerPage
    val cubs = (0 until numCuboids).map { j =>
      val dims = (1 to 10).map { i =>
        val total = 5
        val numBits = Random.nextInt(total + 1)
        val selectedBits = Util.collect_n(numBits, () => Random.nextInt(total)).toSet
        val bitsValues = bitsToBools(total, selectedBits)
        DimensionBits("Dim" + i, bitsValues)
      }
      CuboidDef(dims)
    }
    Future.successful(GetChosenCuboidsResponse(cubs.take(cubsInPage)))
  }

  override def deleteChosenCuboid(in: DeleteSelectedCuboidArgs): Future[Empty] = {
    Future.successful(Empty())
  }
  override def getAvailableCuboids(in: GetCuboidsArgs): Future[GetAvailableCuboidsResponse] = {
    val numCuboids = Random.nextInt(100)
    val cubsInPage = in.rowsPerPage
    val cubs = (0 until numCuboids).map { j =>
      val dims = (1 to 10).map { i =>
        val total = 5
        val numBits = Random.nextInt(total + 1)
        val selectedBits = Util.collect_n(numBits, () => Random.nextInt(total)).toSet
        val bitsValues = bitsToBools(total, selectedBits)
        DimensionBits("Dim" + i, bitsValues)
      }
      val isChosen = Random.nextBoolean()
      GetAvailableCuboidsResponse.ManualSelectionCuboidDef(dims, isChosen)
    }
    Future.successful(GetAvailableCuboidsResponse(cubs.take(cubsInPage)))
  }
  override def manuallyUpdateCuboids(in: ManuallyUpdateCuboidsArgs): Future[Empty] = {
    Future.successful(Empty())
  }
  override def materializeCuboids(in: MaterializeArgs): Future[Empty] = {
    Future.successful(Empty())
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
    val bits = (0 until timeBits).map{i => Random.nextBoolean()}
    val result = Array(GetRenameTimeResponse.ResultRow("0", 0, 10), GetRenameTimeResponse.ResultRow("1", 20, 0))
    val string = if(isComplete) "Column Changed between X and Y" else ""
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
    val dims = (1 to 10).map{i =>
      val dim= "Dim" + i
      val numBits = 6
      val levels = (1 to numBits).map{j => dim + "L" +j}
      SelectDataCubeForQueryResponse.DimHierarchy(dim, numBits, levels)}
    val response = SelectDataCubeForQueryResponse(dims)
    Future.successful(response)
  }
  override def getValuesForSlice(in: GetSliceValuesArgs): Future[GetSliceValueResponse] = {
    val values = (1 to 100).map(i => "V"+i)
    val shownValues = values.filter(_.contains(in.searchText)).take(in.numRowsInPage)
    val response = GetSliceValueResponse(shownValues)
    Future.successful(response)
  }

  // TODO: Implement something to get and post filters

  override def getFilters(in: Empty): Future[GetFiltersResponse] = ???

  override def setValuesForSlice(in: SetSliceValuesArgs): Future[Empty] = ???

  override def startQuery(in: QueryArgs): Future[QueryResponse] = {
    import QueryState._
    import QueryResponse._
    stats.clear()
    cubsFetched = 1
    numPrepared = Random.nextInt(100)
    prepareCuboids = (0 until numPrepared).map{j =>
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
    series += SeriesData("Linear", (1 to 10).map{i => XYpoint("P" +i, i + cubsFetched)})
    series += SeriesData("Quadratic", (1 to 10).map{i => XYpoint("P" +i, math.pow(i + cubsFetched/10.0, 2).toFloat)})
    series += SeriesData("Log", (1 to 10).map{i => XYpoint("P" +i, math.log(i + cubsFetched).toFloat)})
    val response = QueryResponse(0, shownCuboids, 0, series) // TODO?: Convert stats?
    Future.successful(response)
  }
  override def continueQuery(in: Empty): Future[QueryResponse] = {

    import QueryState._
    import QueryResponse._
    stats.clear()
    cubsFetched += 1
    stats += ("PrepareTime" -> cubsFetched.toString)
    stats += ("FetchTime" -> (2 * cubsFetched).toString)
    stats += ("SolveTime" -> (3 * cubsFetched).toString)
    stats += ("Error" -> (0.4 - cubsFetched / 10.0).toString)


    val series = collection.mutable.ArrayBuffer[SeriesData]()
    series += SeriesData("Linear", (1 to 10).map { i => XYpoint("P" + i, i + cubsFetched) })
    series += SeriesData("Quadratic", (1 to 10).map { i => XYpoint("P" + i, math.pow(i + cubsFetched / 10.0, 2).toFloat) })
    series += SeriesData("Log", (1 to 10).map { i => XYpoint("P" + i, math.log(i + cubsFetched).toFloat) })
    val response = QueryResponse(0, shownCuboids, 0, series) // TODO?: Convert stats?
    Future.successful(response)
  }

  // TODO: Implement something
  override def getPreparedCuboids(in: GetPreparedCuboidsArgs): Future[GetPreparedCuboidsResponse] = ???
}
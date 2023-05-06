package frontend.service

import akka.actor.ActorSystem
import akka.grpc.scaladsl.WebHandler
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.Materializer
import com.typesafe.config.ConfigFactory

import scala.concurrent.{ExecutionContext, Future}
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
  override def getBaseCuboids(in: Empty): Future[BaseCuboidResponse] = {
    Future.successful(BaseCuboidResponse(List("NYC", "SSB", "AirlineDelay")))
  }
}
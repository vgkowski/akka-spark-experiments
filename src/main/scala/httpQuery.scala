import akka.actor.{ActorSystem, ActorLogging, Actor, ActorRef}
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.Await

import akka.stream.{ActorMaterializer, Materializer}

import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.{HttpResponse, HttpRequest}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.unmarshalling.Unmarshal

//import spray.json.DefaultJsonProtocol

class HttpQueryActor(spark: ActorRef, config : QueryApiConfig) extends Actor with ActorLogging{

  implicit val system = context.system
  implicit val executor = system.dispatcher
  implicit val materializer = ActorMaterializer()
  implicit val timeout = Timeout(config.httpTimeout seconds)

  val logger = Logging(system, getClass)

  def receive : Actor.Receive = {
    case e =>
  }

  import QueryApiJsonProtocol._

  val routes = {
    logRequestResult("akka-http-microservice") {
      path("top-k-query") {
        get {
          entity(as[TopKLocationQuery]) { topKLocation =>
            log.info(s"Querying {} top {} location.", topKLocation.msisdn,topKLocation.k )
            val futureResponse = spark ? topKLocation
            complete {
              Await.result(futureResponse, timeout.duration).asInstanceOf[TopKLocationResponse]
            }
          } ~
          entity(as[List[TopKLocationQuery]]) { topKLocationList =>
            topKLocationList.foreach { topKLocation =>
              log.info(s"Querying {} top {} location.", topKLocation.msisdn,topKLocation.k )
              spark ! topKLocation
            }
            complete {
              topKLocationList
            }
          }
        }
      }
    }
  }
  Http().bindAndHandle(routes, config.httpInterface, config.httpPort)
}

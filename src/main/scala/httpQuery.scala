import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
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
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.unmarshalling.Unmarshal

//import spray.json.DefaultJsonProtocol

object HttpQueryActor{
  def props(spark:ActorRef,config:QueryApiConfig) : Props= Props(new HttpQueryActor(spark,config))
}

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
        post {
          entity(as[TopKLocationQuery]) { topKLocation =>
            log.info("Querying {} top {} location.", topKLocation.msisdn,topKLocation.k )
            val futureResponse = spark ? topKLocation
            complete {
              Await.result(futureResponse, timeout.duration).asInstanceOf[TopKLocationResponse]
            }
          } ~
          entity(as[List[TopKLocationQuery]]) { topKLocationList =>
            topKLocationList.foreach { topKLocation =>
              log.info("Querying {} top {} location.", topKLocation.msisdn,topKLocation.k )
              spark ! topKLocation
            }
            complete {
              topKLocationList
            }
          }
        }
      } ~
      path("usage-query") {
        post {
          entity(as[UsageKPIQuery]) { usageKPI =>
            log.info("Querying {} top {} location.", usageKPI.msisdn )
            val futureResponse = spark ? usageKPI
            complete {
              Await.result(futureResponse, timeout.duration).asInstanceOf[UsageKPIResponse]
            }
          } ~
            entity(as[List[UsageKPIQuery]]) { usageKPIList =>
              usageKPIList.foreach { usageKPI =>
                log.info("Querying {} top {} location.", usageKPI.msisdn )
                spark ! usageKPI
              }
              complete {
                usageKPIList
              }
            }
        }
      }
    }
  }
  Http().bindAndHandle(routes, config.httpInterface, config.httpPort)
}

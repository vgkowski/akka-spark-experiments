import java.util.concurrent.TimeoutException

import akka.actor.SupervisorStrategy.{Escalate, Restart, Stop}
import akka.actor._
import akka.routing.BalancingPool
import akka.util.Timeout
import akka.cluster.{Member, Metric, NodeMetrics, Cluster}
import akka.cluster.ClusterEvent._

import scala.concurrent.Future
//import scala.concurrent.ExecutionContext.Implicits.global
import scala.math._
import scala.util.control.NonFatal
import scala.concurrent.duration._

import org.apache.spark.SparkConf
import org.apache.spark.streaming._

import akka.cluster.seed._

sealed trait LifeCycleEvent extends Serializable
case object OutputStreamInitialized extends LifeCycleEvent
case object NodeInitialized extends LifeCycleEvent


final class QueryApiNodeGuardian(config :QueryApiConfig) extends Actor with ActorLogging {

  import SupervisorStrategy._
  import akka.pattern.gracefulStop

  val cluster = Cluster(context.system)

  protected val sparkConf = new SparkConf().setAppName(getClass.getSimpleName)
    .setMaster(config.sparkMaster)
    .set("spark.cassandra.connection.host", config.cassandraHosts)
    .set("spark.cleaner.ttl", config.sparkCleanerTtl.toString)
    .set("spark.mesos.executor.home", config.sparkMesosExecutorHome)
    .set("spark.driver.cores", config.sparkDriverCores)
    .set("spark.driver.memory", config.sparkDriverMemory)
    .set("spark.mesos.coarse",config.sparkMesosCoarse)
    .set("spark.executor.memory",config.sparkExecutorMemory)
    .set(
      config.sparkMesosCoarse match{
        case "true" => "spark.cores.max"
        case "false" => "spark.mesosExecutor.cores"
      },
      config.sparkMesosCoarse match{
        case "true" => config.sparkMesosCoresMax.get
        case "false" => config.sparkMesosExecutorCores.get
      }
    )
    .set("spark.jars",config.sparkJars)


  /** Creates the Spark Streaming context. */
  protected val ssc = new StreamingContext(sparkConf, Milliseconds(config.sparkStreamingBatchInterval))

  /** subscribe to cluster changes, re-subscribe when restart. */
  override def preStart(): Unit = {
    ZookeeperClusterSeed(context.system).join()
    cluster.subscribe(self, classOf[ClusterDomainEvent])
    log.info("Starting at {}", cluster.selfAddress)
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
    log.info("Node {} shutting down.", cluster.selfAddress)
  }

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minute) {
      case _: ActorInitializationException => Stop
      case _: IllegalArgumentException => Stop
      case _: IllegalStateException    => Restart
      case _: TimeoutException         => Escalate
      case _: Exception                => Escalate
    }


  def receive : Actor.Receive = {
    case MemberUp(member) => watch(member)
    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}", member.address, previousStatus)
    case ClusterMetricsChanged(forNode) =>
      forNode collectFirst { case m if m.address == cluster.selfAddress =>
        log.debug("{}", filter(m.metrics))
      }
    case OutputStreamInitialized => initialize()
    case _: MemberEvent =>
  }

  /** Initiated when node receives a [[akka.cluster.ClusterEvent.MemberUp]]. */
  private def watch(member: Member): Unit = {
    log.debug("Member [{}] joined cluster.", member.address)
  }

  def filter(nodeMetrics: Set[Metric]): String = {
    val filtered = nodeMetrics collect { case v if v.name != "processors" => s"${v.name}:${v.value}" }
    s"NodeMetrics[${filtered.mkString(",")}]"
  }

  def initialize(): Unit = {
    log.info(s"Node is transitioning from 'uninitialized' to 'initialized'")
    context.system.eventStream.publish(NodeInitialized)

    ssc.checkpoint(config.sparkCheckpointDir)
    ssc.start()

    //context become initialized
  }

  /*protected def gracefulShutdown(listener: ActorRef): Unit = {
    implicit val timeout = Timeout(5.seconds)
    val status = Future.sequence(context.children.map(shutdown))
    listener ! status
    log.info(s"Graceful shutdown completed.")
  }

  /** Executes [[akka.pattern.gracefulStop( )]] on `child`.*/
  private def shutdown(child: ActorRef)(implicit t: Timeout): Future[Boolean] =
    try gracefulStop(child, t.duration + 1.seconds) catch {
      case NonFatal(e) =>
        log.error("Error shutting down {}, cause {}", child.path, e.toString)
        Future(false)
    }*/

  //cluster.joinSeedNodes(Vector(cluster.selfAddress))

  // launch the spark actors
  val spark = context.actorOf(Props(classOf[SparkQueryActor],config,ssc), "spark-query")

  // launch the http actors after the spark actor is initialized
  cluster registerOnMemberUp {
    /* As http data is received, send query to spark. */
    log.info(s"name of the actorsystem for http receiver {}",context.system.name)
    context.actorOf(Props(classOf[HttpQueryActor],spark,config), "http-query")

    log.info("Starting request listening on {}.", cluster.selfAddress)
  }
}
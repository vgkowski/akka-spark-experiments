import java.util.concurrent.TimeoutException

import akka.actor.SupervisorStrategy.{Escalate, Restart, Stop}
import akka.actor._
import akka.routing.BalancingPool

sealed trait LifeCycleEvent extends Serializable
case object OutputStreamInitialized extends LifeCycleEvent
case object NodeInitialized extends LifeCycleEvent


final class QueryApiNodeGuardian(conf :QueryApiConfig) extends Actor with ActorLogging {

  import SupervisorStrategy._
  import akka.pattern.gracefulStop
  //import context.dispatcher
  val config=conf
  println("name of actorsystem in node guardian"+context.system.name)
  val cluster = Cluster(context.system)


  /** subscribe to cluster changes, re-subscribe when restart. */
  override def preStart(): Unit = {
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

  cluster.joinSeedNodes(Vector(cluster.selfAddress))

  // launch the kafka actors
  val router = context.actorOf(BalancingPool(1).props(
    Props(new KafkaPublisherActor(config))), "kafka-publisher")

  // launch the http actors
  cluster registerOnMemberUp {
    /* As http data is received, publishes to Kafka. */
    log.info(s"name of the actorsystem for http receiver {}",context.system.name)
    context.actorOf(BalancingPool(1).props(
      Props(new HttpReceiverActor(router,conf))), "http-receiver")

    log.info("Starting data ingestion on {}.", cluster.selfAddress)
  }
}
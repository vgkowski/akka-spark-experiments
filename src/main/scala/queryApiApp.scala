import akka.actor.{Props, ActorSystem, PoisonPill}

object QueryApiApp extends App{
  val conf = new QueryApiConfig(args(0))
  val system = ActorSystem("ingestionApi",conf.config)

  val guardian = system.actorOf(Props(classOf[QueryApiNodeGuardian],conf), "node-guardian")
  //override val config= ConfigFactory.parseFile(new File(args(0)))

  system.registerOnTermination {
    guardian ! PoisonPill
  }
}
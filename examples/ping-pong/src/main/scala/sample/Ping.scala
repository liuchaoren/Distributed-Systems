package sample

import akka.actor._

class Ping(pong: ActorRef) extends Actor {
  var count = 0
  def incrementAndPrint: Unit = { count += 1; println("ping") }
  var listener: Option[ActorRef] = None
  def receive = {
    case StartMessage() =>
      listener = Some(sender)
      incrementAndPrint
      pong ! "ping"
    case "pong" => 
      incrementAndPrint
      if(listener.isDefined)
        listener.get ! DoneMessage()
  }
}

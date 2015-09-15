package sample

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.Await

case class StartMessage()
case class DoneMessage()

object PingPongTest extends App {
  val system = ActorSystem("PingPongSystem")
  implicit val timeout = Timeout(60 seconds)
  val pong = system.actorOf(Props(classOf[Pong]), "pong")
  val ping = system.actorOf(Props(classOf[Ping], pong), "ping")
  // kick off
  val future = ask(ping, StartMessage()).mapTo[DoneMessage]
  // wait to be done
  val done = Await.result(future, 60 seconds)
  system.shutdown()
}

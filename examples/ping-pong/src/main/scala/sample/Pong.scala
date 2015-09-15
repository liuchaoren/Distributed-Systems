package sample

import akka.actor._

class Pong extends Actor {
  def receive = {
    case "ping" =>
      println("pong")
      sender ! "pong"
  }
}

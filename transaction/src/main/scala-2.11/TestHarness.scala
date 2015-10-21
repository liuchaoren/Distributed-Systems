/**
 * Created by Chaoren on 10/20/15.
 */
package transaction

import scala.concurrent.duration._
import scala.concurrent.Await

import akka.actor.{Actor, ActorSystem, ActorRef, Props}
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout

object TestHarness {
  val numNodes = 100
  val numStores = 100
  val burstSize = 100
//  val opsPerNode = 10000
  val system = ActorSystem("Transactions")
  implicit val timeout = Timeout(60 seconds)

  // Service tier: create app servers and a Seq of per-node Stats
  val master = KVAppService(system, numNodes, numStores, burstSize)

  def main(args: Array[String]): Unit = run()

  def run(): Unit = {
//    val s = System.currentTimeMillis
    initializeStores
    runUntilDone
//    val runtime = System.currentTimeMillis - s
//    val throughput = (burstSize * numNodes)/runtime
//    println(s"Done in $runtime ms ($throughput Kops/sec)")
    system.shutdown()
  }

  def runUntilDone() = {
    val future = ask(master, Join()).mapTo[String]
    master ! Start()
    val done = Await.result(future, 60 seconds)
    println(s"$done")
  }

  def initializeStores() = {
    val future = ask(master, Join()).mapTo[String]
    master ! storesPopu()
    val done = Await.result(future, 60 seconds)
    println(s"$done")
  }

}


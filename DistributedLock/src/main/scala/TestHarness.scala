package DistributedLock

import akka.actor.ActorSystem
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.Await

import akka.actor.{Actor, ActorSystem, ActorRef, Props}
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout

object TestHarness {
  val numUsers = 10
  val numOperations = 1000
  // val burstSize = 1000
  // val opsPerNode = 10000
  val system = ActorSystem("Lock")
  implicit val timeout = Timeout(30 seconds)


  // Service tier: create app servers and a Seq of per-node Stats
  val server = system.actorOf(LockServer.props(), "LockServer")
  val users = for (i <- 0 until numUsers)
    yield system.actorOf(UserServer.props(server, system), "UserServer" + i)
  val master = system.actorOf(LoadMaster.props(numUsers, numOperations, users), "LoadMaster")

  def main(args: Array[String]): Unit = run()

  def run(): Unit = {
    // for (i <- 0 until numOperations) {
    //   val pickedUser = users(userGenerator.nextInt(numUsers))
    //   val pickedLock = lockList(lockGenerator.nextInt(lockList.length))
    //   pickedUser ! Command(pickedLock)
    // }
    val s = System.currentTimeMillis
    runUntilDone
    // val runtime = System.currentTimeMillis - s
    // val throughput = (opsPerNode * numNodes)/runtime
    // println(s"Done in $runtime ms ($throughput Kops/sec)")
    system.shutdown()
  }

  def runUntilDone() = {
    val future = ask(master, Join()).mapTo[String]
    master ! Start(numOperations)
    val done = Await.result(future, 30 seconds)
    println(s"Final stats: $done")
  }

}

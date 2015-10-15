package DistributedLock

import akka.actor.{Actor, ActorSystem, ActorRef, Props}
import akka.event.Logging

sealed trait LoadMasterAPI
case class Start(numOperations: Int) extends LoadMasterAPI
case class JobSuccess(name:String) extends LoadMasterAPI
case class JobFail(nama:String) extends LoadMasterAPI
case class Join() extends LoadMasterAPI

/** LoadMaster is a singleton actor that generates load for the app service tier, accepts acks from
  * the app tier for each command burst, and terminates the experiment when done.  It uses the incoming
  * acks to self-clock the flow of commands, to avoid swamping the mailbox queues in the app tier.
  * It also keeps running totals of various Stats returned by the app servers with each burst ack.
  * A listener may register via Join() to receive a message when the experiment is done.
  *
  * @param numNodes How many actors/servers in the app tier
  * @param servers ActorRefs for the actors/servers in the app tier
  * @param burstSize How many commands per burst
  */

class LoadMaster (val numUsers:Int, val numOperations: Int, val users: Seq[ActorRef]) extends Actor {
  val log = Logging(context.system, this)
  var active: Boolean = true
  var listener: Option[ActorRef] = None
  var opsWaiting = numOperations
  var opsSuccess = 0
  var opsFail = 0
  // var maxPerNode: Int = 0

  val lockList = List("lock1", "lock2", "lock3", "lock4", "lock5")

  // val serverStats = for (s <- servers) yield new Stats

  def receive = {
    case Start(numOperations: Int) =>
      val userGenerator = new scala.util.Random
      val lockGenerator = new scala.util.Random
      // log.info("Master starting bursts")
      // maxPerNode = totalPerNode
      for (i <- 0 until numOperations) {
        val pickedUser = users(userGenerator.nextInt(numUsers))
        val pickedLock = lockList(lockGenerator.nextInt(lockList.length))
        // val pickedLock = lockList(0)
        pickedUser ! Command(pickedLock)
        // burst(s)
      }

    case JobSuccess(name:String) =>
      opsWaiting = opsWaiting - 1
      opsSuccess = opsSuccess + 1
      println(s"$opsWaiting operations left to be done")
      if (opsWaiting == 0)
        deactivate(opsSuccess, opsFail)

    case JobFail(name:String) =>
      opsWaiting = opsWaiting - 1
      opsFail = opsFail + 1
      println(s"$opsWaiting operations left to be done")
      if (opsWaiting == 0)
        deactivate(opsSuccess, opsFail)


    case Join() =>
      listener = Some(sender)
  }

  // def burst(server: ActorRef): Unit = {
  //   for (i <- 1 to burstSize)
  //     server ! Command()
  // }

  def deactivate(opsSuccess: Int, opsFail: Int) = {
    // active = false
    // val total = new Stats
    // serverStats.foreach(total += _)
    if (listener.isDefined)
      listener.get ! s"$opsSuccess operations are done, and $opsFail operations fail"
  }
}

object LoadMaster {
  def props(numUsers:Int, numOperations:Int, users: Seq[ActorRef]): Props = {
    Props(classOf[LoadMaster], numUsers, numOperations, users)
  }
}


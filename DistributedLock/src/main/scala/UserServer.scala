package DistributedLock
// import lock
// import lease

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.{Actor, ActorSystem, ActorRef, Props}
sealed trait UserServerAPI
case class Recall(name:String) extends UserServerAPI
case class Command(name:String) extends UserServerAPI

// class RingCell(var prev: BigInt, var next: BigInt)
// class AnyMap extends scala.collection.mutable.HashMap[String, lock]

/**
 * RingService is an example app service for the actor-based KVStore/KVClient.
 * This one stores RingCell objects in the KVStore.  Each app server allocates new
 * RingCells (allocCell), writes them, and reads them randomly with consistency
 * checking (touchCell).  The allocCell and touchCell commands use direct reads
 * and writes to bypass the client cache.  Keeps a running set of Stats for each burst.
 *
 * @param myNodeID sequence number of this actor/server in the app tier
 * @param numNodes total number of servers in the app tier
 * @param storeServers the ActorRefs of the KVStore servers
 * @param burstSize number of commands per burst
 */

class UserServer (lockServer: ActorRef, system: ActorSystem) extends Actor {
  // val generator = new scala.util.Random
  val lockStore = new LockClient(self, lockServer, system)
  // val dirtycells = new AnyMap
  // val localWeight: Int = 70

  // var stats = new Stats
  // var allocated: Int = 0
  // var endpoints: Option[Seq[ActorRef]] = None


  def receive() = {
    case Recall(name:String) =>
      // println(s"got a recall requirement!")
      finishupAndReleaseLock(name)
    //     rwcheck(myNodeID, new RingCell(0,0))
    case Command(name:String) =>
      command(name, sender)
    // case View(e) =>
    //   endpoints = Some(e)
  }

  private def command(name:String, sender: ActorRef) = {
    val myname = self.path.name
    println(s"$myname is asked to work on $name")
    val lock = lockStore.acquireLock(name)
    if (lock.isDefined) {
      doSomeThing(name)
      lockStore.releaseLocktoCache(name)
      sender ! JobSuccess(name)
      println(s"$name is done by $myname")

    } else {
      sender ! JobFail(name)
      // println(s"$name cannot be acquired at this time, try later")
    }

  }

  private def doSomeThing(name:String) = {
    val myname = self.path.name
    println(s"$myname is working on variable $name")
  }

  private def finishupAndReleaseLock(name: String)  = {
    // if (lockStore.cache.get(name).isDefined) {
    // val myname = self.path.name
    // println(s"$myname is finishing up some work")
    // cache -= name
    lockStore.releaseLocktoServer(name)
    // } else {
    // println(s"$name is not defined, so no release")
    // }

  }
}


object UserServer {
  def props(lockServer: ActorRef, system: ActorSystem): Props = {
    Props(classOf[UserServer], lockServer, system)
  }
}

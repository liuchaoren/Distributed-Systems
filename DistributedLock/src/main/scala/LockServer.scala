package DistributedLock

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.{Actor, ActorSystem, ActorRef, Props}
import scala.collection.mutable.Queue


case class lease(var status: Boolean, var holder: Option[ActorRef], var leaseStart: Option[Long], val leaseTerm: Long)
case class lock(val Name: String, var lease: lease)
sealed trait LockServerAPI
case class Acquire(name: String, requirer: ActorRef) extends LockServerAPI
case class Release(name: String) extends LockServerAPI
// class RingMap extends scala.collection.mutable.HashMap[BigInt, RingCell]

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

class LockServer extends Actor {

  private val free = true
  private val locked = false
  private val termLen = 1000
  private val gracePeriod = 200
  private val timeBeforeRecall = 0
  // val generator = new scala.util.Random
  // val cellstore = new KVClient(storeServers)
  // val dirtycells = new AnyMap
  // val localWeight: Int = 70

  // var stats = new Stats
  // var allocated: Int = 0
  // var endpoints: Option[Seq[ActorRef]] = None
  private val lockStore = new scala.collection.mutable.HashMap[String, lock]


  def receive() = {
    case Acquire(name: String, realsender: ActorRef) =>
      acquireLock(name, realsender, sender)
    // val sendername = sender.path.name
    // println(s"haha $sender")
    //     rwcheck(myNodeID, new RingCell(0,0))
    case Release(name: String) =>
      releaseLock(name)
    // case View(e) =>
    //   endpoints = Some(e)
  }

  // private def find(Name:String): Tuple2 = {
  //   var found = false
  //   val foundlock:Option[lock] = None
  //   for (val lock <- lockStore.iterator) {
  //     if (lock.Name == Name) {
  //       found = true
  //       foundlock = lock
  //     }
  //   }
  //   return (found, foundlock)
  // }


  private def acquireLock(name:String, realsender: ActorRef, sender: ActorRef) = {
    // println(s"haha $requirer")
    val acquiredLock = lockStore.get(name)
    if (acquiredLock.isDefined) {
      // lease = foundlock.get.lease
      if (acquiredLock.get.lease.status == free || System.currentTimeMillis - acquiredLock.get.lease.leaseStart.get > acquiredLock.get.lease.leaseTerm + gracePeriod) {
        // lease.status = locked
        // lease.holder = sender
        // lease.leaseStart = System.currentTimeMillis
        // lease.leaseTerm = termLen
        val updatedLock = lock(name, lease(locked, Some(realsender), Some(System.currentTimeMillis), termLen))
        println(s"haha $updatedLock")
        lockStore.put(name, updatedLock)
        sender ! Some(updatedLock)
        println(s"$name is returned to $realsender")
      } else {
        // if (System.currentTimeMillis - acquiredLock.get.lease.leaseStart.get > timeBeforeRecall) {
        //   recallLock(name, acquiredLock.get.lease.holder.get)
        // }
        sender ! None
      }
    } else {
      lockStore.put(name, lock(name, lease(locked, Some(realsender), Some(System.currentTimeMillis), termLen)))
      sender ! lockStore.get(name)
      println(s"$name is returned to $realsender")
    }
  }

  private def releaseLock(name: String) = {
    // println()
    val releasedLock = lockStore.get(name)
    if (releasedLock.isDefined) {
      println(s"I am releasing lock $name")
      lockStore.put(name, lock(name, lease(free, None, None, termLen)))
    } else {
      print ("Error: there is no lock to be released")
    }
  }


  private def recallLock(name: String, lockHolder: ActorRef) = {
    val holdername = lockHolder.path.name
    println(s"lockServer ask $holdername to release $name")
    lockHolder ! Recall(name)
  }
}



object LockServer {
  def props(): Props = {
    Props(classOf[LockServer])
  }
}

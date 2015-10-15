package DistributedLock
// import DistributedLock.lock
// import DistributedLock.lease

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Scheduler
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.util.{Success, Failure}
import java.util.concurrent.TimeoutException
// import system.dispatcher

import akka.actor.{Actor, ActorSystem, ActorRef, Props, Scheduler}
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.ExecutionContext.Implicits.global



class CacheMap extends scala.collection.mutable.HashMap[String, lock]

/**
 * KVClient implements a client's interface to a KVStore, with an optional writeback cache.
 * Instantiate one KVClient for each actor that is a client of the KVStore.  The values placed
 * in the store are of type Any: it is up to the client app to cast to/from the app's value types.
 * @param stores ActorRefs for the KVStore actors to use as storage servers.
 */

class LockClient (user: ActorRef, lockStore: ActorRef, system: ActorSystem) {
  val cache = new CacheMap
  implicit val timeout = Timeout(1 seconds)
  private val intervalAsk = 1000


  def acquireLock (Name: String): Option[lock] = {
    var alock = cache.get(Name)
    if (alock.isDefined) {
      return alock
    } else {
      alock =  directAcquireLock(user, Name, lockStore)
      if (alock.isDefined)
        cache.put(Name, alock.get)
      return alock
    }
  }



  def directAcquireLock(user: ActorRef, Name: String, lockStore: ActorRef): Option[lock] = {
    // println(s"$lockStore")
    val future = ask(lockStore, Acquire(Name, user)).mapTo[Option[lock]]
    var alock: Option[lock] = None
    // val alock = Await.result(future, timeout.duration)
    try {
      // println(s"haha")
      alock = Await.result(future, timeout.duration)
    } catch {
      case e:TimeoutException => println(s"timeout")
    }
    // future.onComplete {
    //   case Success(result) => alock = result
    //   case Failure(ex) => println("Error in directAcquireLock")
    // }
    // var trialTime = 10
    // while (lock.isEmpty && trialTime > 0) {
    //   system.scheduler.scheduleOnce(intervalAsk milliseconds) {
    //     val future = ask(lockStore, Acquire(Name)).mapTo[Option[lock]]
    //     val lock = Await.result(future, timeout.duration)
    //   }
    //   trialTime = trialTime - 1
    // }
    // println(s"the lock is $alock")
    return alock
  }

  def purge() = {
    cache.clear()
  }


  def releaseLocktoCache(Name: String) = {
    val alock = cache.get(Name)
    // println(s"haha $alock.getClass")
    // lockStore ! releaseLock(Name)
    val curr = System.currentTimeMillis
    val start = alock.get.lease.leaseStart.get
    val term = alock.get.lease.leaseTerm
    // println(s"current time is $curr")
    // println(s"start time is $start")
    // println(s"term len is $term")
    if (System.currentTimeMillis - alock.get.lease.leaseStart.get > alock.get.lease.leaseTerm) {
      releaseLocktoServer(Name)
    }
  }

  def releaseLocktoServer(Name: String) = {
    if (cache.get(Name).isDefined) {
      cache -= Name
      lockStore ! Release(Name)
    }
    // lockStore ! releaseLock(Name)
  }


}

/**
 * Created by Chaoren on 10/17/15.
 */
package transaction

import akka.actor.{ActorRef, Actor, Props}
import scala.collection.mutable.HashMap
import scala.collection.mutable.Queue

sealed trait KVStoreAPI
case class Put(key: BigInt, value: Any) extends KVStoreAPI
case class Get(key: BigInt) extends KVStoreAPI
case class commitVote(key: BigInt) extends KVStoreAPI
case class ReleaseKey(key:BigInt) extends  KVStoreAPI
case class AcquireAndGet(key:BigInt) extends KVStoreAPI
case class PutAndRelease(key:BigInt, cell:Any) extends KVStoreAPI


/**
 * KVStore is a local key-value store based on actors.  Each store actor controls a portion of
 * the key space and maintains a hash of values for the keys in its portion.  The keys are 128 bits
 * (BigInt), and the values are of type Any.
 */

class KVStore extends Actor {
  private val store = new scala.collection.mutable.HashMap[BigInt, Any]
  private val lockQueue = new HashMap[BigInt, Queue[ActorRef]]

  override def receive = {
    case Put(key, cell) =>
      sender ! store.put(key, cell)
    case Get(key) =>
      sender ! store.get(key)
    case ReleaseKey(key) =>
//      println("ReleaseKey")
      keyRelease(key)
    case AcquireAndGet(key) =>
      if (lockQueue.get(key).isEmpty) {
        lockQueue.put(key, new Queue[ActorRef])
      }
      if (lockQueue.get(key).get.isEmpty) {
        lockQueue.get(key).get.enqueue(sender)
        sender ! store.get(key)
      }
      else {
        lockQueue.get(key).get.enqueue(sender)
      }
    case PutAndRelease(key, cell) =>
      store.put(key, cell)
//      println("PutAndRelease")
      keyRelease(key)


    case commitVote(key) =>
      sender ! "yes"
  }


  private def keyRelease(key: BigInt): Unit = {
//    println(lockQueue.get(key))
    if (lockQueue.get(key).isDefined) {
      lockQueue.get(key).get.dequeue()
      if (!lockQueue.get(key).get.isEmpty)
        lockQueue.get(key).get.front ! store.get(key)
    } else
      println("Error")
  }
}

object KVStore {
  def props(): Props = {
    Props(classOf[KVStore])
  }
}

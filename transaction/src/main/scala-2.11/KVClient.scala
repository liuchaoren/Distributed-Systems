/**
 * Created by Chaoren on 10/17/15.
 */


package transaction

import java.util.concurrent.TimeoutException

//import com.sun.tools.internal.xjc.reader.xmlschema.bindinfo.BIGlobalBinding

import scala.concurrent.duration._
import scala.concurrent.Await

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout

class AnyMap extends scala.collection.mutable.HashMap[BigInt, Any]

/**
 * KVClient implements a client's interface to a KVStore, with an optional writeback cache.
 * Instantiate one KVClient for each actor that is a client of the KVStore.  The values placed
 * in the store are of type Any: it is up to the client app to cast to/from the app's value types.
 * @param stores ActorRefs for the KVStore actors to use as storage servers.
 */

class KVClient (stores: Seq[ActorRef]) {
  private val cache = new AnyMap
  implicit val timeout = Timeout(5 seconds)

  import scala.concurrent.ExecutionContext.Implicits.global

  /** Cached read */
  def read(key: BigInt): Option[Any] = {
    var value = cache.get(key)
    if (value.isEmpty) {
      value = directRead(key)
      if (value.isDefined)
        cache.put(key, value.get)
    }
    value
  }

  /** Cached write: place new value in the local cache, record the update in dirtyset. */
  def write(key: BigInt, value: Any, dirtyset: AnyMap) = {
    cache.put(key, value)
    dirtyset.put(key, value)
  }

  /** Push a dirtyset of cached writes through to the server. */
  def push(dirtyset: AnyMap) = {
    val futures = for ((key, v) <- dirtyset)
      directWrite(key, v)
    dirtyset.clear()
  }

  /** Purge every value from the local cache.  Note that dirty data may be lost: the caller
    * should push them.
    */
  def purge() = {
    cache.clear()
  }

  /** Direct read, bypass the cache: always a synchronous read from the store, leaving the cache unchanged. */
  def directRead(key: BigInt): Option[Any] = {
    val future = ask(route(key), Get(key)).mapTo[Option[Any]]
    Await.result(future, timeout.duration)
  }

  /** Direct write, bypass the cache: always a synchronous write to the store, leaving the cache unchanged. */
  def directWrite(key: BigInt, value: Any) = {
    val future = ask(route(key), Put(key, value)).mapTo[Option[Any]]
    Await.result(future, timeout.duration)
  }

  def acquire(key:BigInt) = {
    val future = ask(route(key), AcquireAndGet(key)).mapTo[Option[Any]]
    Await.result(future, timeout.duration)
  }

  import java.security.MessageDigest

  /** Generates a convenient hash key for an object to be written to the store.  Each object is created
    * by a given client, which gives it a sequence number that is distinct from all other objects created
    * by that client.
    * @param nodeID Which client
    * @param seqID Unique sequence number within client
    */
  def hashForKey(nodeID: Int, seqID: Int): BigInt = {
    val label = "Node" ++ nodeID.toString ++ "+Cell" ++ seqID.toString
    val md: MessageDigest = MessageDigest.getInstance("MD5")
    val digest: Array[Byte] = md.digest(label.getBytes)
    BigInt(1, digest)
  }

  /**
   * @param key A key
   * @return An ActorRef for a store server that stores the key's value.
   */
  private def route(key: BigInt): ActorRef = {
    stores((key % stores.length).toInt)
  }

  def begin(Akey: BigInt, Bkey: BigInt): Tuple2[Option[Any], Option[Any]] = {
    var AkeyValue: Option[Any] = None
    var BkeyValue: Option[Any] = None
    if (Akey < Bkey) {
//        println("I am reading")
        AkeyValue = acquire(Akey)
        BkeyValue = acquire(Bkey)
    }
    else {
        BkeyValue = acquire(Bkey)
        AkeyValue = acquire(Akey)
    }
    return Tuple2(AkeyValue, BkeyValue)
  }

  def voteOnAKey(key: BigInt): String = {
    var voteYes = 0
    var voteNo = 0
    val S = route(key)
    val futureA = ask(S, commitVote(key)).mapTo[String]
    try
      Await.result(futureA, timeout.duration)
    catch {
      //      case TimeoutException => "no"
      case _ => "no"
    }
  }

  def Abort(Akey: BigInt, Bkey: BigInt): Unit = {
    route(Akey) ! ReleaseKey(Akey)
    route(Bkey) ! ReleaseKey(Bkey)
    purge()
  }


  def submit(Akey: BigInt, AkeyValue: Double, Bkey: BigInt, BkeyValue: Double): Unit = {
    route(Akey) ! PutAndRelease(Akey, AkeyValue)
    route(Bkey) ! PutAndRelease(Bkey, BkeyValue)
    purge()
  }
}
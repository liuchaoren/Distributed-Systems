/**
 * Created by Chaoren on 10/20/15.
 */
package transaction

import akka.actor.{Actor, ActorSystem, ActorRef, Props}

//class RingCell(var prev: BigInt, var next: BigInt)
//class RingMap extends scala.collection.mutable.HashMap[BigInt, RingCell]


class myTransactionService(val myNodeID: Int, val numNodes: Int, storeServers: Seq[ActorRef], burstSize: Int) extends Actor {
  val generator = new scala.util.Random
  val cellstore = new KVClient(storeServers)
  val dirtycells = new AnyMap
  val localWeight: Int = 70
  val loadNum = 100
  var numDone = 0

//  var stats = new Stats
  var allocated: Int = 0
  var endpoints: Option[Seq[ActorRef]] = None


  def receive() = {
    case LoadStore() =>
      var key: BigInt = 0
      var value = 0.0
      for (i <- 0 until loadNum) {
        key = cellstore.hashForKey(myNodeID, i)
        value = generator.nextFloat()
        cellstore.directWrite(key, value)
//        println(s"start $key:$myNodeID:$i:$value")
      }
      sender ! popAck()



    case Command() =>
      incomingTs(sender)
    case View(e) =>
      endpoints = Some(e)
  }


  private def incomingTs(master: ActorRef) = {
    stats.messages += 1
    if (stats.messages >= burstSize) {
      master ! BurstAck(myNodeID, stats)
      stats = new Stats
    }
  }
//
//  private def allocCell() = {
//    val key = chooseEmptyCell
//    var cell = directRead(key)
//    assert(cell.isEmpty)
//    val r = new RingCell(0, 1)
//    stats.allocated += 1
//    directWrite(key, r)
//  }
//
//  private def chooseEmptyCell(): BigInt =
//  {
//    allocated = allocated + 1
//    cellstore.hashForKey(myNodeID, allocated)
//  }
//
//  /*
//   * By modifying RingCells in place we may be racing with our k/v servers.  XXX
//   */
//  private def touchCell() = {
//    stats.touches += 1
//    val key = chooseActiveCell
//    val cell = directRead(key)
//    if (cell.isEmpty) {
//      stats.misses += 1
//    } else {
//      val r = cell.get
//      if (r.next != r.prev + 1) {
//        stats.errors += 1
//        r.prev = 0
//        r.next = 1
//      } else {
//        r.next += 1
//        r.prev += 1
//      }
//      directWrite(key, r)
//    }
//  }
//
//  private def chooseActiveCell(): BigInt = {
//    val chosenNodeID =
//      if (generator.nextInt(100) <= localWeight)
//        myNodeID
//      else
//        generator.nextInt(numNodes - 1)
//
//    val cellSeq = generator.nextInt(allocated)
//    cellstore.hashForKey(chosenNodeID, cellSeq)
//  }
//
//  private def rwcheck(key: BigInt, value: RingCell) = {
//    directWrite(key, value)
//    val returned = read(key)
//    if (returned.isEmpty)
//      println("rwcheck failed: empty read")
//    else if (returned.get.next != value.next)
//      println("rwcheck failed: next match")
//    else if (returned.get.prev != value.prev)
//      println("rwcheck failed: prev match")
//    else
//      println("rwcheck succeeded")
//  }
//
//  private def read(key: BigInt): Option[RingCell] = {
//    val result = cellstore.read(key)
//    if (result.isEmpty) None else
//      Some(result.get.asInstanceOf[RingCell])
//  }
//
//  private def write(key: BigInt, value: RingCell, dirtyset: AnyMap): Option[RingCell] = {
//    val coercedMap: AnyMap = dirtyset.asInstanceOf[AnyMap]
//    val result = cellstore.write(key, value, coercedMap)
//    if (result.isEmpty) None else
//      Some(result.get.asInstanceOf[RingCell])
//  }
//
//  private def directRead(key: BigInt): Option[RingCell] = {
//    val result = cellstore.directRead(key)
//    if (result.isEmpty) None else
//      Some(result.get.asInstanceOf[RingCell])
//  }
//
//  private def directWrite(key: BigInt, value: RingCell): Option[RingCell] = {
//    val result = cellstore.directWrite(key, value)
//    if (result.isEmpty) None else
//      Some(result.get.asInstanceOf[RingCell])
//  }
//
//  private def push(dirtyset: AnyMap) = {
//    cellstore.push(dirtyset)
//  }
}

object myTransactionService {
  def props(myNodeID: Int, numNodes: Int, storeServers: Seq[ActorRef], burstSize: Int): Props = {
    Props(classOf[myTransactionService], myNodeID, numNodes, storeServers, burstSize)
  }
}


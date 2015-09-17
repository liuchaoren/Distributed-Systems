package rings

import akka.actor.{Actor, ActorSystem, ActorRef, Props}

class RingCell(var prev: BigInt, var next: BigInt)
class RingMap extends scala.collection.mutable.HashMap[BigInt, RingCell]

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

class GroupServer (val myNodeID: Int, val numNodes: Int, storeServers: Seq[ActorRef], burstSize: Int) extends Actor {
  val generator = new scala.util.Random
  val cellstore = new KVClient(storeServers)
  val dirtycells = new AnyMap
  val groupJoinWeight: Int = 70
  val groupJoinTotalWeight: Int = 100
  val numberofgroups: Int = 4
  val groupidseq: Seq[Int] = for (i <- 0 until numberofgroups)
  val messageSender: Option[ActorRef] = None

  var stats = new Stats
  // var groupID: Int = 0
  var endpoints: Option[Seq[ActorRef]] = None


  def receive() = {
      case Prime() =>
        {
          for (groupID <- groupidseq) {
            sample = generator.NextInt(groupJoinTotalWeight)
            if (sample < groupJoinWeight) 
              GroupJoin(groupID)
            else 
              GroupLeave(groupID)
          }
        }
      //     rwcheck(myNodeID, new RingCell(0,0))
      // case Command(groupID: Int) =>
     
      case Command() =>
        incoming(sender)
        command
      case View(e) =>
        endpoints = Some(e)
      case Play() => 
        play(sender)
  }



  private def GroupJoin(groupID: Int) {
    val key = cellstore.hashForKey(myNodeID, groupID)
    val groupInd : Boolean = true
    directWrite(key, groupInd)
        // allocCell
  }

  private def GroupLeave(groupID: Int) { 
    val key = cellstore.hashForKey(myNodeID, groupID)
    val groupInd : Boolean = false
    directWrite(key, groupInd)
  }

  private def command() = {
    val selectedGroup = generator.nextInt(numberofgroups)
    for (i <- 0 until numNodes) {
      val key = cellstore.hashForKey(i, selectedGroup)
      val value = directRead(key)
      if (value == true) 
        endpoints(i)  ! Play()
    }
  }

  private def paly(sender:ActorRef) = {
    val senderID = sender.myNodeID 
    println(s"GroupServer $senderID ask $myNodeID to play")
  }

  private def incoming(master: ActorRef) = {
    stats.messages += 1
    if (stats.messages >= burstSize) {
      master ! BurstAck(myNodeID, stats)
      stats = new Stats
    }
  }

  // private def allocCell() = {
  //   val key = chooseEmptyCell
  //   var cell = directRead(key)
  //   assert(cell.isEmpty)
  //   val r = new RingCell(0, 1)
  //   stats.allocated += 1
  //   directWrite(key, r)
  // }

  // private def chooseEmptyCell(): BigInt =
  // {
  //   allocated = allocated + 1
  //   cellstore.hashForKey(myNodeID, allocated)
  // }

  /*
   * By modifying RingCells in place we may be racing with our k/v servers.  XXX
   */
  // private def touchCell() = {
  //   stats.touches += 1
  //   val key = chooseActiveCell
  //   val cell = directRead(key)
  //   if (cell.isEmpty) {
  //     stats.misses += 1
  //   } else {
  //     val r = cell.get
  //     if (r.next != r.prev + 1) {
  //       stats.errors += 1
  //       r.prev = 0
  //       r.next = 1
  //     } else {
  //       r.next += 1
  //       r.prev += 1
  //     }
  //     directWrite(key, r)
  //   }
  // }

  // private def chooseActiveCell(): BigInt = {
  //   val chosenNodeID =
  //     if (generator.nextInt(100) <= localWeight)
  //       myNodeID
  //     else
  //       generator.nextInt(numNodes - 1)

  //   val cellSeq = generator.nextInt(allocated)
  //   cellstore.hashForKey(chosenNodeID, cellSeq)
  // }

  // private def rwcheck(key: BigInt, value: RingCell) = {
  //   directWrite(key, value)
  //   val returned = read(key)
  //   if (returned.isEmpty)
  //     println("rwcheck failed: empty read")
  //   else if (returned.get.next != value.next)
  //     println("rwcheck failed: next match")
  //   else if (returned.get.prev != value.prev)
  //     println("rwcheck failed: prev match")
  //   else
  //     println("rwcheck succeeded")
  // }

  // private def read(key: BigInt): Option[RingCell] = {
  //   val result = cellstore.read(key)
  //   if (result.isEmpty) None else
  //     Some(result.get.asInstanceOf[RingCell])
  // }

  // private def write(key: BigInt, value: RingCell, dirtyset: AnyMap): Option[RingCell] = {
  //   val coercedMap: AnyMap = dirtyset.asInstanceOf[AnyMap]
  //   val result = cellstore.write(key, value, coercedMap)
  //   if (result.isEmpty) None else
  //     Some(result.get.asInstanceOf[RingCell])
  }

  private def directRead(key: BigInt): Option[Boolean] = {
    val result = cellstore.directRead(key)
    if (result.isEmpty) None else
      Some(result.get.asInstanceOf[Boolean])
  }

  private def directWrite(key: BigInt, value: Boolean): Option[Boolean] = {
    val result = cellstore.directWrite(key, value)
    if (result.isEmpty) None else
      Some(result.get.asInstanceOf[Boolean])
  }

  // private def push(dirtyset: AnyMap) = {
  //   cellstore.push(dirtyset)
  // }
}

object GroupServer {
  def props(myNodeID: Int, numNodes: Int, storeServers: Seq[ActorRef], burstSize: Int): Props = {
    Props(classOf[GroupService], myNodeID, numNodes, storeServers, burstSize)
  }
}

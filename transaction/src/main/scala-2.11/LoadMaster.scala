/**
 * Created by Chaoren on 10/20/15.
 */
package transaction

import akka.actor.{Actor, ActorSystem, ActorRef, Props}
import akka.event.Logging

sealed trait LoadMasterAPI
case class Start(maxPerNode: Int) extends LoadMasterAPI
//case class BurstAck(senderNodeID: Int, stats: Stats) extends LoadMasterAPI
case class Join() extends LoadMasterAPI
case class storesPopu() extends LoadMasterAPI
case class popAck() extends LoadMasterAPI


class LoadMaster (val numNodes: Int, val servers: Seq[ActorRef], burstSize: Int) extends Actor {
//  val log = Logging(context.system, this)
  var activeInit: Boolean = true
  var activeLoad: Boolean = true
  var listener: Option[ActorRef] = None
  var nodesActiveInit = numNodes
  var nodesActiveLoad = numNodes
  // num of key-value pairs in each store
//  var maxPerNode: Int = 0

//  val serverStats = for (s <- servers) yield new Stats

  def receive = {
    case storesPopu() =>
      for (s <- servers) {
        populates(s)
      }


    case popAck() =>
      nodesActiveInit -= 1
      if (nodesActiveInit == 0) {
        deactivateInit()
      }

    case Start() =>
////      log.info("Master starting bursts")
////      maxPerNode = totalPerNode
      for (s <- servers) {
        burst(s)
      }

//    case BurstAck(senderNodeID: Int, stats: Stats) =>
//      serverStats(senderNodeID) += stats
//      val s = serverStats(senderNodeID)
//      if (s.messages == maxPerNode) {
//        println(s"node $senderNodeID done, $s")
//        nodesActive -= 1
//        if (nodesActive == 0)
//          deactivate()
//      } else {
//        if (active)
//          burst(servers(senderNodeID))
//      }

    case Join() =>
      listener = Some(sender)
  }

  def populates(s: ActorRef) = {
    s ! LoadStore()
  }


  def burst(server: ActorRef): Unit = {
    for (i <- 1 to burstSize)
      server ! Command()
  }

  def deactivateInit() = {
    if(listener.isDefined)
      listener.get ! "done"
  }
//  def deactivate() = {
//    active = false
//    val total = new Stats
//    serverStats.foreach(total += _)
//    if (listener.isDefined)
//      listener.get ! total
//  }
}

object LoadMaster {
  def props(numNodes: Int, servers: Seq[ActorRef], burstSize: Int): Props = {
    Props(classOf[LoadMaster], numNodes, servers, burstSize)
  }
}


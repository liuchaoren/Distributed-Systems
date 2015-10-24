/**
 * Created by Chaoren on 10/20/15.
 */
package transaction

import akka.actor.{Actor, ActorSystem, ActorRef, Props}
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.pattern.ask
import akka.util.Timeout

//class RingCell(var prev: BigInt, var next: BigInt)
//class RingMap extends scala.collection.mutable.HashMap[BigInt, RingCell]


class myTransactionService(val myNodeID: Int, val numNodes: Int, storeServers: Seq[ActorRef], burstSize: Int) extends Actor {
  import scala.concurrent.ExecutionContext.Implicits.global
  val generator = new scala.util.Random
  val cellstore = new KVClient(storeServers)
  val dirtycells = new AnyMap
  val localWeight: Int = 70
  val loadNum = 100
  var numTDone = 0
  var numTFail = 0
//  var voteYes = 0
//  var voteNo = 0
  implicit val timeout = Timeout(5 seconds)

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
      sender ! TransAck()


//    case voteCommit() =>
//      println(s"I received a commit vote")
//      voteYes += 1
//
//    case voteAbort() =>
//      voteNo += 1

    case View(e) =>
      endpoints = Some(e)
  }


  private def incomingTs(master: ActorRef) = {
    val servername = self.path.name
//    println(burstSize)
    for (i <- 0 until burstSize) {

//      voteNo = 0
//      voteYes = 0
      var Akey:BigInt = 0
      var Bkey:BigInt = 0
      while (Akey == Bkey) {
        Akey = cellstore.hashForKey(generator.nextInt(numNodes), generator.nextInt(loadNum))
        Bkey = cellstore.hashForKey(generator.nextInt(numNodes), generator.nextInt(loadNum))
      }
//      println(s"haha $Akey")
//      println(s"haha $Bkey")

      val transfer = generator.nextFloat()
//      println(s"$Akey")
      val values = cellstore.begin(Akey, Bkey)
      val AkeyValue = values._1.get.asInstanceOf[Double] - transfer
      val BkeyValue = values._2.get.asInstanceOf[Double] + transfer

      val voteA = cellstore.voteOnAKey(Akey, i)
//            println(s"I get voteA is $voteA")
      if (voteA != "yes") {
        cellstore.Abort(Akey, Bkey)
        numTFail += 1
        println(s"$servername fails to complete its transaction $i")
      }
      else {
        val voteB = cellstore.voteOnAKey(Bkey, i)
        if (voteB != "yes") {
          cellstore.Abort(Akey, Bkey)
          numTFail += 1
          println(s"$servername fails to complete its transaction $i")
        }
        else {
//          println(s"I am submitting T")
          cellstore.submit(Akey, AkeyValue, Bkey, BkeyValue)
          numTDone += 1
          println(s"$servername completes its transaction $i successfully")
//          println(s"I am done with T")
        }
      }
    }
  }


}

object myTransactionService {
  def props(myNodeID: Int, numNodes: Int, storeServers: Seq[ActorRef], burstSize: Int): Props = {
    Props(classOf[myTransactionService], myNodeID, numNodes, storeServers, burstSize)
  }
}


/**
 * Created by Chaoren on 10/20/15.
 */
package transaction

import akka.actor.{ActorSystem, ActorRef, Props}
import org.omg.IOP.TransactionService

sealed trait AppServiceAPI
case class Prime() extends AppServiceAPI
case class Command() extends AppServiceAPI
case class Play() extends AppServiceAPI
case class getNodeNum() extends AppServiceAPI
case class View(endpoints: Seq[ActorRef]) extends AppServiceAPI
case class LoadStore() extends AppServiceAPI

object KVAppService {
  def apply(system: ActorSystem, numNodes: Int, numStores: Int, loadEach: Int): ActorRef = {

    /** Storage tier: create K/V store servers */
    val stores = for (i <- 0 until numStores)
      yield system.actorOf(KVStore.props(), "DataStore" + i)

    /** Service tier: create app servers */
    val servers = for (i <- 0 until numNodes)
      yield system.actorOf(myTransactionService.props(i, numNodes, stores, loadEach), "TransactionServer" + i)

    /** if you want to initialize a different service instead, that previous line might look like this:
      * yield system.actorof(groupserver.props(i, numnodes, stores, ackeach), "groupserver" + i)
      * for that you need to implement the groupserver object and the companion actor class.
      * following the "rings" example.
      */


    /** tells each server the list of servers and their actorrefs wrapped in a message. */
    for (server <- servers)
      server ! View(servers)

    /** load-generating master */
    val master = system.actorOf(LoadMaster.props(numNodes, servers, loadEach), "LoadMaster")
    master
  }
}

//#package
package sample
//#package

//#config
import akka.remote.testkit.MultiNodeConfig
import akka.actor._
import com.typesafe.config.ConfigFactory

object PingPongConfig extends MultiNodeConfig {
  val node1 = role("node1")
  val node2 = role("node2")
 
  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "akka.remote.RemoteActorRefProvider"
    akka.remote.log-remote-lifecycle-events = off
    """))
}
//#config

//#spec
import akka.remote.testkit.MultiNodeSpec
import akka.testkit.ImplicitSender
import akka.actor.{Props, Actor}

class PingPongSpecMultiJvmNode1 extends PingPongSpec
class PingPongSpecMultiJvmNode2 extends PingPongSpec

class PingPongSpec extends MultiNodeSpec(PingPongConfig)
  with STMultiNodeSpec with ImplicitSender {

  import PingPongConfig._

  def initialParticipants = roles.size

  def getName: Unit = {
    val name = System.getProperty("pingpong.jvmname")
    println(s"${name} is up!")
  }

  "PingPong" must {

    "send/receive messages" in {
      runOn(node1) {
        getName
        enterBarrier("deployed")
        val ponger = system.actorSelection(node(node2) / "user" / "ponger")
        println("ping a ponger")
        ponger ! "ping"
        expectMsg("pong")
      }

      runOn(node2) { 
        system.actorOf(Props[Pong], "ponger")
        getName
        enterBarrier("deployed")
      }

      enterBarrier("finished")
    }
  }
}
//#spec

package dada.group

import event._
import scala.collection.mutable.Map
import dds.Topic
import dds.pub._
import dds.sub._

import dds.qos._
import dada.detail._
import dds.event.{DataAvailable, LivelinessChanged}
import util.logging.Logged
import dada.Config._
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{TimeUnit, Semaphore}

/**
 * This class implement an group with an eventually consistent view.
 */


object EventualGroup {
  def groupTopic = Topic[TMemberInfo]("MemberInfo")
  private val subMap = Map[Int, Subscriber]()
  private val pubMap = Map[Int, Publisher]()
  private val groupMap = Map[Int, EventualGroup]()

  def apply(gid: Int): EventualGroup =  {

    groupMap.getOrElse(gid, {
      val g = new EventualGroup(gid)
      groupMap += (gid -> g)
      g
    })
  }

  def groupPublisher(gid: Int): Publisher = {
    val pub = pubMap.getOrElse(gid, {
      val p: Publisher = Publisher(groupTopic.dp, PublisherQos(gid.toString))
      pubMap += (gid -> p)
      p
    })
    pub
  }

  def groupSubscriber(gid: Int): Subscriber = {
    subMap.getOrElse(gid, {
      val s = Subscriber(groupTopic.dp, SubscriberQos(gid.toString))
      subMap += (gid -> s)
      s
    })
  }

  val drQos = DataReaderQos() + History.KeepLast(1) + Reliability.Reliable + Durability.TransientLocal
  val dwQos = DataWriterQos() + History.KeepLast(1) + Reliability.Reliable + Durability.TransientLocal
}



class EventualGroup private (val gid: Int)(implicit Logger: dada.Config.Logger) extends Group {
  private var members = List[(Int, Long)]()
  import EventualGroup._

  private val memberDR = DataReader[TMemberInfo](groupSubscriber(gid), groupTopic, drQos)
  private val memberDW = DataWriter[TMemberInfo](groupPublisher(gid), groupTopic, dwQos)
  private val semaphore = new Semaphore(0)

  private val aliveCount = new AtomicLong(1)

  memberDR.reactions += {
    case DataAvailable(dr) => {
      Logger.debug("onDataAvailable")
      val samples = memberDR take(SampleSelector.AllSamples)
      val zipped = samples.data zip samples.info

      val joined = zipped filter (z => {
        (z._2.valid_data &&
          z._1.status == TMemberStatus.JOINED &&
          z._2.instance_state == DDS.ALIVE_INSTANCE_STATE.value &&
          z._2.sample_state == DDS.NOT_READ_SAMPLE_STATE.value)}) map (z => (z._1.mid, z._2.publication_handle))

      val left = zipped filter(z => {
        (z._2.sample_state == DDS.NOT_READ_SAMPLE_STATE.value &&
          z._2.instance_state == DDS.ALIVE_INSTANCE_STATE.value &&
          z._1.status == TMemberStatus.LEAVED)}) map (z => (z._1.mid, z._2.publication_handle))

      synchronized {
        members = (members filterNot(left contains)) ::: joined.toList
      }

      // There is a new view, thus we notify those that are waiting for it to have a look
      Logger.debug("Releasing View Semaphore. New view size = "+ this.size)
      semaphore.release()

      joined foreach(m => reactions(new MemberJoin(m._1)))
      left foreach(m => reactions(new MemberLeave(m._1)))
    }

    case LivelinessChanged(_, lc) => {
      Logger.debug("Enter onLivelinessChanged")
      val ac = lc.alive_count
      if (ac < aliveCount.getAndSet(ac)) {
        val sm = members find (_._2 == lc.last_publication_handle)
        sm map(m => {
          synchronized {
            members = members filterNot (_ == m)
          }
          semaphore.release()
          reactions(new MemberFailure(m._1))
        })
      }
      Logger.debug("Exit onLivelinessChanged")

    }
  }

  def join(mid: Int) {
    memberDW ! new TMemberInfo(mid, TMemberStatus.JOINED)
  }

  def leave(mid: Int) {
    memberDW ! new TMemberInfo(mid, TMemberStatus.LEAVED)
  }

  def size = members.length

  def view = members map (_._1)

  def waitForViewSize(n: Int) {
    while (this.size != n) {
      Logger.debug("Waiting for view to be established")
      semaphore.acquire()
      Logger.debug("View has been updated to "+ this.size)
    }
  }

  /**
   * Waits for the view size to be established for a bounded amount of time.
   * @param n number of member that have to join the group
   * @param timeout time for which the call will wait for the view to establish in millisec
   */
  def waitForViewSize(n: Int, timeout: Int) {
      if (this.size != n) semaphore.tryAcquire(timeout, TimeUnit.MILLISECONDS)


  }
}

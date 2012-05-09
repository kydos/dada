package dada.group

import dds.Topic
import dds.pub._
import dds.sub._

import dds.qos._
import dada.detail._
import event._
import dds.event.{DataAvailable, LivelinessChanged}
import util.logging.Logged
import dada.Config._
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{TimeUnit, Semaphore}
import collection.mutable.{HashMap, Map}

/**
 * This class implement an group with an eventually consistent view.
 */


object EventualGroup {
  def groupTopic = Topic[TMemberInfo]("MemberInfo")
  def electionTopic = Topic[TEventualLeaderVote]("EventualLeaderVote")
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
  private val leadersScore = HashMap[Int, Int]()
  private val membersVote = HashMap[Int, Int]()
  private var epoch = new AtomicLong(0)

  import EventualGroup._

  private val memberDR = DataReader[TMemberInfo](groupSubscriber(gid), groupTopic, drQos)
  private val memberDW = DataWriter[TMemberInfo](groupPublisher(gid), groupTopic, dwQos)

  private val leaderDR = DataReader[TEventualLeaderVote](groupSubscriber(gid), electionTopic, drQos)
  private val leaderDW = DataWriter[TEventualLeaderVote](groupPublisher(gid), electionTopic, dwQos)

  private val semaphore = new Semaphore(0)

  private val aliveCount = new AtomicLong(1)

  private var currentLeader: Option[Int] = None

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

      joined foreach(m => {
        reactions(new MemberJoin(m._1))
        reactions (new EpochChange(epoch.incrementAndGet()))
      })
      left foreach(m => {
        reactions(new MemberLeave(m._1))
        reactions (new EpochChange(epoch.incrementAndGet()))
      })
    }

    case LivelinessChanged(_, lc) => {
      Logger.debug("Enter onLivelinessChanged")
      val ac = lc.alive_count
      if (ac < aliveCount.getAndSet(ac)) {
        val sm = members find (_._2 == lc.last_publication_handle)
        sm map(m => {
          synchronized {
            members = members filterNot (_ == m)
            Logger.debug("--- Updated Members View ---")
            members.foreach (m => Logger.debug(m.toString))
            Logger.debug("--- * ---")
          }
          semaphore.release()
          val fm = m._1
          reactions(new MemberFailure(fm))
          membersVote.get(fm).map { v =>
            leadersScore.get(v).map { s =>
              leadersScore += (v -> (s - 1))
              s
            }
            v
          }
          membersVote remove (fm)
        })
        reactions (new EpochChange(epoch.incrementAndGet()))
      }

      Logger.debug("Exit onLivelinessChanged")

    }
  }

  leaderDR.reactions += {
    case DataAvailable(_) => {
      (leaderDR take) foreach (vote => {
        Logger.debug("Received vote: ("+ vote.mid + ", "+ vote.lid +", "+ vote.epoch +")")
        if (vote.epoch > epoch.get()) {
          epoch.set(vote.epoch)
          reactions (new EpochChange(epoch.get()))
        }
        // record the vote
        val pvl = membersVote.get(vote.mid).getOrElse(-1)

        membersVote += (vote.mid -> vote.lid)
        // update the score
        if (pvl != vote.lid) {
          val score = leadersScore.get(vote.lid).getOrElse(0) + 1
          leadersScore += (vote.lid -> score)
          if (pvl != -1) {
            val s = leadersScore(pvl) - 1
            leadersScore += (pvl -> s)
          }
        }
      })
      // have we reached a quorum yet?
      (leadersScore find (_._2 >= (this.size/2 +1))).map { l =>
        val pastLeader = currentLeader
        currentLeader = Some(l._1)
        if (currentLeader != pastLeader)
          reactions (new NewLeader(currentLeader))
        l
      }
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

  /**
   * Returns the current leader, if any elected yet
   * @return the current leader, or None if one has not been elected yet.
   */
  def leader: Option[Int] = currentLeader

  /**
   * Proposes a member as eventual leader.
   * @param lid the member id of the proposed leader
   */
  def proposeLeader(mid: Int, lid: Int) {
    Logger.debug("Member: "+ mid + " is proposing Leader: " + lid +" for epoch: "+ epoch)
    leaderDW ! new TEventualLeaderVote(epoch.get(), mid, lid)
  }
}

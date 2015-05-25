package dada.group

import dds._
import dds.config.DefaultEntities.{defaultDomainParticipant, defaultPolicyFactory}
import dds.prelude._
import dada.detail._
import event._
import dada.Config._
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{TimeUnit, Semaphore}
import org.omg.dds.sub.ViewState

import collection.mutable.{HashMap, Map}
import java.util.concurrent.atomic.AtomicReference
import dada.concurrent.synchronisers._
import scala.language.postfixOps
import data.util._

/**
 * This class implement an group with an eventually consistent view.
 */


object EventualGroup {
  def groupTopic = Topic[TMemberInfo]("MemberInfo")
  def electionTopic = Topic[TEventualLeaderVote]("EventualLeaderVote")
  private var subMap = Map[Int, org.omg.dds.sub.Subscriber]()
  private var pubMap = Map[Int, org.omg.dds.pub.Publisher]()
  private var groupMap = Map[Int, EventualGroup]()


  def apply(gid: Int): EventualGroup =  synchronized {
    groupMap.getOrElse(gid, {
      val g = new EventualGroup(gid)
      groupMap = groupMap + (gid -> g)
        g
    })
  }


  def groupPublisher(gid: Int): org.omg.dds.pub.Publisher = synchronized {
    val pub = pubMap.getOrElse(gid, {
      val pubQos = PublisherQos().withPolicy(Partition(gid.toString))
      val p: org.omg.dds.pub.Publisher = Publisher(groupTopic.getParent, pubQos)
      pubMap = pubMap + (gid -> p)
      p
    })
    pub
  }

  def groupSubscriber(gid: Int): org.omg.dds.sub.Subscriber = synchronized {
    subMap.getOrElse(gid, {
      val subQos = SubscriberQos().withPolicy(Partition(gid.toString))
      val s = Subscriber(groupTopic.getParent, subQos)
      subMap += (gid -> s)
      s
    })
  }

  import dds.config.DefaultEntities.{defaultPub, defaultSub}
  val drQos = DataReaderQos().withPolicies(
    History.KeepLast(1),
    Reliability.Reliable,
    Durability.TransientLocal
  )
  val dwQos = DataWriterQos().withPolicies(
    History.KeepLast(1),
    Reliability.Reliable,
    Durability.TransientLocal
  )
}



class EventualGroup private (val gid: Int)(implicit Logger: dada.Config.Logger) extends Group {
  private val membersRef = new AtomicReference(List[TMemberInfo]())
  private val leadersScore = Map[Int, Int]()
  private val membersVote = Map[Int, Int]()
  private var epoch = new AtomicLong(0)

  import EventualGroup._

  private val memberDR = DataReader[TMemberInfo](groupSubscriber(gid), groupTopic, drQos)
  private val memberDW = DataWriter[TMemberInfo](groupPublisher(gid), groupTopic, dwQos)

  private val leaderDR = DataReader[TEventualLeaderVote](groupSubscriber(gid), electionTopic, drQos)
  private val leaderDW = DataWriter[TEventualLeaderVote](groupPublisher(gid), electionTopic, dwQos)

  private val semaphore = new Semaphore(0)

  private val aliveCount = new AtomicLong(1)

  private var currentLeader: Option[Int] = None
  import scala.collection.JavaConversions._
  import org.omg.dds.sub.{InstanceState, SampleState}

  memberDR listen  {
    case DataAvailable(_) => {
      Logger.debug("onDataAvailable")
      val anySample = memberDR.getParent.createDataState()
        .withAnyInstanceState()
        .withAnySampleState()
        .withAnyViewState()

      val samples = memberDR
        .select()
          .dataState(anySample)
        .read().toList

      Logger.debug(samples.foldLeft("\n")((xs, x) => xs + show(x) + "\n"))

      val joined = samples.filter { z =>
        (z.getData != null
          && z.getData.status.value == TMemberStatus.JOINED.value
          && z.getInstanceState.value == InstanceState.ALIVE.value
          && z.getSampleState.value == SampleState.NOT_READ.value)
      } map(_.getData)

      val left = samples.filter { z =>
        (z.getData.status.value == TMemberStatus.LEAVED.value
        && z.getInstanceState.value != InstanceState.ALIVE.value)
      } map(_.getData)

      val failed = samples.filter { z =>
        z.getInstanceState.value != InstanceState.ALIVE.value
      } map (_.getData)

      // Clean-up disposed samples from the cache
      val disposedSamples = memberDR.getParent.createDataState()
        .`with`(InstanceState.NOT_ALIVE_DISPOSED).`with`(InstanceState.NOT_ALIVE_NO_WRITERS)
        .`with`(SampleState.READ).`with`(SampleState.READ)
        .`with`(ViewState.NEW).`with`(ViewState.NOT_NEW)

      memberDR
        .select()
        .dataState(disposedSamples)
        .take().toList



      compareAndSet(membersRef) { members => members.filterNot(x => left.map(_.mid).contains(x.mid) || failed.map(_.mid).contains(x.mid)) ::: joined}


      // There is a new view, thus we notify those that are waiting for it to have a look
      Logger.debug("Releasing View Semaphore. New view size = "+ this.size)
      semaphore.release()

      joined foreach(m => {
        react(new MemberJoin(m.mid))
        react(new EpochChange(epoch.incrementAndGet()))
      })

      left foreach(m => {
        react(new MemberLeave(m.mid))
        react(new EpochChange(epoch.incrementAndGet()))
      })

      failed foreach { m =>
        println("Failed Member: " + m.mid + " - " + m.status.value())
        react (new MemberFailure((m.mid)))
        react(new EpochChange(epoch.incrementAndGet()))
      }
    }
  }

  leaderDR.listen {
    case DataAvailable(_) => {
      (leaderDR take) foreach (sample => {
        val vote = sample.getData
        Logger.debug("Received vote: ("+ vote.mid + ", "+ vote.lid +", "+ vote.epoch +")")
        if (vote.epoch > epoch.get()) {
          epoch.set(vote.epoch)
          react(new EpochChange(epoch.get()))
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
          react(new NewLeader(currentLeader))
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

  def size = membersRef.get.length

  def view = membersRef.get.map (_.mid)

  def waitForViewSize(n: Int): Unit = {
    Logger.debug("Waiting for view to be established")
    while (this.size != n)
      semaphore.acquire()
    Logger.debug("View has been updated to "+ this.size)

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

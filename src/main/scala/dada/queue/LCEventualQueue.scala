package dada.queue

import dada.detail._
import dds.Topic
import dada.group.Group
import dada.clock.LogicalClock
import dds.event.DataAvailable
import java.io.{ObjectInputStream, ObjectOutputStream, ByteArrayInputStream, ByteArrayOutputStream}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.Semaphore
import dada.Config
import collection.mutable.{PriorityQueue, SynchronizedPriorityQueue, Map}
import dds.pub.{Publisher, DataWriter}
import dds.sub.{Subscriber, DataReader}
import dada.group.Group._
import dds.qos._
import dada.queue.LCEventualQueue._


class LCEventualEnqueue[T](val qname: String,
                              val mid: Int,
                              val gid: Int)
                             (implicit logger: Config.Logger)
  extends Enqueue[T] {

  protected val pub = Publisher(groupPublisher(gid).dp, PublisherQos(gid.toString + qname))
  protected val elemDW = DataWriter[TQueueElement](pub, queueElemTopic, dwQos)
  protected var ts = LogicalClock(0, mid)

  def enqueue(t: T) {
    val baos = new ByteArrayOutputStream()
    val oos  = new ObjectOutputStream(baos)
    oos.writeObject(t)
    val elem = new TQueueElement(ts, baos.toByteArray)
    baos.reset()
    synchronized {
      ts = ts inc()
    }
    logger.trace(Console.BLUE+"ENQUEUE_S = {}"+ Console.RESET, ts)
    elemDW ! elem

    oos.close()
    baos.close()
  }
}

object LCEventualQueue {
  val queueElemTopic = Topic[TQueueElement]("QueueElement")
  val queueCommandTopic = Topic[TQueueCommand]("QueueCommand")

  val dwQos = DataWriterQos() + Reliability.Reliable + History.KeepAll
  val drQos = DataReaderQos() + Reliability.Reliable + History.KeepAll

}

/**
 * This class provides an "Eventual" Distributed Queue abstraction implemented
 * by using a variation of the Lamport's Mutual Exclusion algorithm based on
 * logical clocks.
 *
 * Notice that this class is not thread-safe! In other terms there is an underlying
 * assumption that call to <code>dequeue</code> are serialized. The Queue could be
 * easily extended to make it thread-safe, the protocol remains the same but the implementation
 * needs to keep track of pending concurrent request for this specific object.
 * We might add this in future releases, but for the time being if you need to share a queue
 * over different threads or different process there is no difference, you just create a new
 * instance!
 *
 * @author <a href="mailto:angelo@icorsaro.net">Angelo Corsaro</a>
 * @author <a href="mailto:sara@icorsaro.net">Sara Tucci</a>
 *
 */
class LCEventualQueue[T](qname: String,
                         mid: Int,
                         gid: Int,
                         val rn: Int)
                        (implicit logger: Config.Logger)
  extends LCEventualEnqueue[T](qname, mid, gid) with Queue[T] { self =>
  import Group._
  import LCEventualQueue._
  import LogicalClock._


  private val sub = Subscriber(groupPublisher(gid).dp, SubscriberQos(gid.toString + qname))

  private val elemDR = DataReader[TQueueElement](sub, queueElemTopic, drQos)

  private val qcmdDW = DataWriter[TQueueCommand](pub, queueCommandTopic, dwQos)
  private val qcmdDR = DataReader[TQueueCommand](sub, queueCommandTopic, drQos)


  private val lcRevOrd = new Ordering[LogicalClock] {
    def compare(x: LogicalClock, y: LogicalClock) = - x.compare(y)
  }
  private val elemRevOrd = new Ordering[TQueueElement] {
    def compare(x: TQueueElement, y: TQueueElement) = - x.ts.compare(y.ts)
  }
  private var elems = new PriorityQueue[TQueueElement]()(elemRevOrd)
  private val requests = new SynchronizedPriorityQueue[LogicalClock]()(lcRevOrd)

  private var myRequest = LogicalClock.Infinite

  private var receivedAcks = new AtomicInteger(0)
  private var synchDequeue = new AtomicBoolean(false)
  private val synchDequeueSemaphore = new Semaphore(0)

  // Queue containing pops for samples we've not received yet.
  private var popQueue = List[LogicalClock]()

  private val dequeueSemaphore = new Semaphore(0)


  private def cmd2Str(cmd: TQueueCommand) =
    "("+ cmd.kind.value + ", "+cmd.mid+", <"+ cmd.ts.ts +", "+cmd.ts.mid+">)"

  elemDR.reactions += {
    case DataAvailable(_) => {
      logger.trace(Console.BLUE + "LCEventualQueue:elemDR.reactions - DataAvailable" + Console.RESET)
      synchronized {
        (elemDR take) foreach (elems.enqueue(_))
      }
      if (synchDequeue.get()) {
        logger.trace(Console.BLUE + "LCEventualQueue:elemDR.reactions - Releasing <sdqueue>" + Console.RESET)
        synchDequeueSemaphore.release()
      }
    }
  }

  qcmdDR.reactions += {
    case DataAvailable(_) => {
      (qcmdDR take) foreach { cmd =>
        cmd.kind match {

          case TCommandKind.DEQUEUE if (cmd.mid != mid) => {
            logger.trace(Console.RED +"DEQUEUE_R {}" + Console.RESET, cmd2Str(cmd))
            logger.trace(Console.RED +"myRequest ts = {}" + Console.RESET, myRequest)
            synchronized {
              val lck = math.max(ts.ts, cmd.ts.ts) +1
              ts = LogicalClock(lck, mid)
            }

            if (cmd.ts < myRequest) {
              val ack = new TQueueCommand(TCommandKind.ACK, cmd.ts.mid, ts)
              logger.trace(Console.GREEN +"DEQUEUE_R => ACK_S {}"+ Console.RESET, cmd2Str(ack))
              qcmdDW ! ack
            }
            else {
              requests.enqueue(cmd.ts)
            }
          }

          case TCommandKind.ACK if (cmd.mid == mid) => {
            synchronized {
              val lck = math.max(ts.ts, cmd.ts.ts) +1
              ts = LogicalClock(lck, mid)
            }
            val acks = receivedAcks.incrementAndGet()
            logger.trace(Console.GREEN +"ACK_R ({}) -  {}"+ Console.RESET, acks, cmd2Str(cmd))
            if (acks == rn - 1) {
              receivedAcks.set(0)
              dequeueSemaphore.release()
            }
          }

          case TCommandKind.POP  if (cmd.mid != mid) => {
            logger.trace(Console.YELLOW +"POP_R {}"+ Console.RESET, cmd2Str(cmd))
            synchronized {
              val lck = math.max(ts.ts, cmd.ts.ts) +1
              ts = LogicalClock(lck, mid)

              (elems find (e => TClock2Clock(e.ts) == TClock2Clock(cmd.ts))) match {
                case Some(e) => {
                  val newElems = (elems dequeueAll) filter (e => TClock2Clock(e.ts) != TClock2Clock(cmd.ts))
                  elems.enqueue(newElems: _*)
                }
                case None => {
                  popQueue = cmd.ts :: popQueue
                }
              }
            }
          }
          case _ => {
          }
        }
      }
    }
  }


  def dequeue(): Option[T] = {
    var data: Option[T] = None
    elems.headOption map(head => {
      var done = false;
      synchronized {
        ts = ts inc()
        myRequest = ts
      }
      val cmd = new TQueueCommand(TCommandKind.DEQUEUE, mid, ts)
      qcmdDW ! cmd
      logger.trace(Console.RED_B +"DEQUEUE_S = "+ cmd2Str(cmd) + Console.RESET)
      dequeueSemaphore.acquire()

      // As I get a Pop request before getting an ACK I am sure that the next
      // element on the priority queue can be safely taken.

      synchronized {
        val toPop = (elems filter (e => popQueue.contains(e.ts))).toList
        elems = (elems filterNot(e => toPop.contains(e.ts)))
        popQueue = popQueue filterNot (toPop.contains)
        logger.trace(Console.RED_B + "----------------Pop Queue---------------" + Console.RESET)
        popQueue foreach(e => logger.trace(Console.RED_B + e + Console.RESET))
        logger.trace(Console.RED_B + "----------------------------------------" + Console.RESET)

        // We might get the right to dequeue, but the queue might be empty!
        elems.headOption map(head => {
          val e = elems dequeue()
          val ois = new ObjectInputStream(new ByteArrayInputStream(e.data))
          data = Some(ois.readObject().asInstanceOf[T])
          ois.close()

          // Ask everyone else to pop the element that I am about to consume.
          ts = ts inc ()
          val pop = new TQueueCommand(TCommandKind.POP, mid, e.ts)
          qcmdDW ! pop
          logger.trace(Console.YELLOW +"POP_S {}"+ Console.RESET, cmd2Str(pop))
        })

        myRequest = LogicalClock.Infinite

        // As there is at most once concurrent take for our queue (single-threaded assumption)
        // it is safe to perform a dequeueAll.

        requests.dequeueAll foreach ( r => {
          ts = ts inc()
          val ack = new TQueueCommand(TCommandKind.ACK, r.id, ts)
          qcmdDW ! ack
          logger.trace(Console.YELLOW +"ACK_S {}"+ Console.RESET, cmd2Str(ack))
        })
      }
    })
    data
  }


  def sdequeue() = {
    synchDequeue.set(true)
    while (length == 0) {
      synchDequeueSemaphore.acquire()
    }
    synchDequeue.set(false)
    dequeue()
  }

  def length = elems.length
}

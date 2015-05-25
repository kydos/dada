package dada.group

import dada.Config._
import java.util.concurrent.atomic.AtomicReference
import dada.concurrent.synchronisers._
import dada.event.Event

object Group {
  private var groupMap = Map[Int, Group]()

  def apply(gid: Int) = {
    synchronized {
      groupMap getOrElse (gid, {
        val g = GroupTypeCompanion(gid)
        groupMap = groupMap + (gid -> g)
        g
      })
    }
  }

  def groupPublisher(gid: Int)= GroupTypeCompanion.groupPublisher(gid)

  def groupSubscriber(gid: Int) = GroupTypeCompanion.groupSubscriber(gid)
}
/**
 * This class provides a group abstractions that allows members to
 * join and leave.  Different concrete implementations
 *
 * @author <a href="mailto:angelo@icorsaro.net">Angelo Corsaro</a>
 * @author <a href="mailto:sara@icorsaro.net">Sara Tucci</a>
 */
abstract class Group {

  private val reactionsRef = new AtomicReference(Map[Int, PartialFunction[Event, Any]]())

  /**
   * Add the member with the specific member-id to this group.
   *
   * @param mid the id of the member add to this group
   */
  def join(mid: Int)

  /**
   * Removes the member with the specific member-id from this group.
   *
   * @param mid the id of the member to remove from this group
   */
  def leave(mid: Int)

  /**
   * Returns the group size.
   * @return the currently etimated group size
   */
  def size: Int

  /**
   * Provides the current group view, i.e. the list of its members.
   *
   * @return the list of group members
   */
  def view: List[Int]

  /**
   * Waits for a specific view size to be established.
   *
   * @param n number of member that have to join the group
   */
  def waitForViewSize(n: Int)

  /**
   * Waits for a specific view size to be established or a maximum time to
   * have elapsed.
   *
   * @param n number of member that have to join the group
   * @param timeout time for which the call will wait for the view to establish
   */
  def waitForViewSize(n: Int, timeout: Int)

  /**
   * Returns the current leader, if any elected yet
   * @return the current leader, or None if one has not been elected yet.
   */
  def leader: Option[Int]

  /**
   * Proposes a member as eventual leader.
   * @param mid the member id that is making the proposal
   * @param lid the member proposed as leader
   */
  def proposeLeader(mid: Int, lid: Int)

  /**
   * Attaches a partial function to react to certain events
   */
  def listen(fun: PartialFunction[Event, Any]): Int = {
    val hash = fun.hashCode
    compareAndSet(reactionsRef) { reactions =>  reactions + (hash -> fun)  }
    hash
  }

  def deaf(hash: Int): Unit = compareAndSet(reactionsRef) { reactions => reactions - hash }

  protected def react(e: Event): Unit = {
    val reactions = reactionsRef.get
    reactions.foreach(f => if (f._2.isDefinedAt(e)) f._2(e))
  }
}

package dada.group

import swing.Reactions
import scala.collection.mutable.Map
import dada.Config._


object Group {
  private var groupMap = Map[Int, Group]()

  def apply(gid: Int) = {
    synchronized {
      groupMap getOrElse (gid, {
        val g = EventualGroup(gid)
        groupMap += (gid -> g)
        g
      })

    }
  }

  def groupPublisher(gid: Int)= EventualGroup.groupPublisher(gid)

  def groupSubscriber(gid: Int) = EventualGroup.groupSubscriber(gid)
}
/**
 * This class provides a group abstractions that allows members to join and leave.
 * Different concrete implementations
 *
 * @author <a href="mailto:angelo@icorsaro.net">Angelo Corsaro</a>
 * @author <a href="mailto:sara@icorsaro.net">Sara Tucci</a>
 */
abstract class Group {

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
   * Reactions attached to this group.
   */
  val reactions = new Reactions.Impl
}

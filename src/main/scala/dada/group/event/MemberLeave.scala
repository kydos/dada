package dada.group.event

/**
 * This event provides information concerning a member who has left the
 * group.
 */

case class MemberLeave(val mid: Int) extends dada.event.Event

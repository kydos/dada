package dada.group.event

/**
 * This event informs of a new member joining the group and provides
 * the new member id.
 */
case class MemberJoin(val mid: Int) extends dada.event.Event

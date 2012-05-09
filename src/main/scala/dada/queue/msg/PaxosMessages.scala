package dada.queue.msg

import dada.clock.LogicalClock


/**
 * This class defines a dequeue request from a client
 * @param sn the sequence number associated to this request
 */
case class DequeRequest(sn: LogicalClock)

/**
 * Represents the result of a deque operation.
 * @param value the value resulting on a dequeue
 * @tparam T the type of the value dequeued
 */
case class Result[T](value: Option[T])


/**
 * The Phase1A message is used to ensure that we are indeed the leader
 * and that we are not being preempted by anybody else
 * @param bn our current ballot number
 */
case class Phase1A(bn: LogicalClock)

/**
 * The Phase1B message is used to communicate the ballot at which
 * the acceptor is at, and what he has accepted so far.
 * @param abn the current ballot
 * @param values the list containing the couple of ballot and accepted values.
 */
case class Phase1B(abn: LogicalClock, values: List[(LogicalClock, LogicalClock)])


/**
 * This message is used to inform the acceptors of the value they are supposed
 * to agree on.
 * @param bn the current ballot
 * @param esn the sample that we want to dequeue
 */
case class Phase2A(bn: LogicalClock, esn: LogicalClock)

/**
 * The Phase2B message is used by the acceptor to acknowledge
 * the proposal or to notify a "preemption" by another leader.
 * @param abn
 */
case class Phase2B(abn: LogicalClock)

/**
 * This message ratifies a decision for assiging a given slot to a given
 * client request
 * @param csn client sequence number representing the deque request
 * @param esn element sequence number, representing the element in the queue to give
 *            back to the client
 */
case class DequeDecision(csn: LogicalClock, esn: LogicalClock)

package dada.queue

import dada.Config._

object Enqueue {
  def apply[T](name: String, mid: Int, gid: Int) = new EnqueueType[T](name, mid, gid)
}
trait Enqueue[T] {
  /**
   * Enqueues an element on this queue.
   *
   * @param t the elements to add to the queue
   */
  def enqueue(t: T)
}

trait Dequeue[T] {
  /**
   * Removes an element from the queue. Which element
   * depends from the implementation as a queue could decide for
   * performance reasons not to match the enqueue order with the
   * dequeue order.
   *
   * @return an element of the queue, or <code>None</code> if the queue
   *         is empty.
   */
  def dequeue(): Option[T]

  /**
   * Removes an element from the queue. Which element
   * depends from the implementation as a queue could decide for
   * performance reasons not to match the enqueue order with the
   * dequeue order.
   *
   * Notice that this method will wait for the queue to be non-empty and then
   * will try to perform a dequeue. However, due to concurrency this method can
   * still return a <code>None</code>!
   *
   * @return an element of the queue, or <code>None</code> if the queue
   *         is empty.
   */
  def sdequeue(): Option[T]

  /**
   * Returns the size of the queue. Beware that even if the queue is non-empty
   * you are not guaranteed to be able to take an element out of it. Why?
   * Well, because this is a distributed queue and other processes/threads might
   * take stuff out of it before you manage to.
   *
   * @return the queue size
   */
  def length: Int

  /**
   * Checks whether the queue is empty.
   *
   * @return true if the queue is empty, false otherwise
   */
  def isEmpty: Boolean = length == 0

}


object Queue {
  def apply[T](name: String, mid: Int, gid: Int, rn: Int) = new QueueType[T](name, mid, gid, rn)
}

/**
 * This class defines an abstract distributed Queue with multiple concurrent producers
 * and multiple concurrent consumers.
 *
 * Notice that although this queue is a distributed queue, its use when sharing a single
 * instance across multiple thread is guaranteed to be safe. Each individual thread/process
 * should have its own "local" queue instance.
 *
 * It is also worth noticing that being the queue "distributed" you are not supposed
 * to make assumptions on where its elements are stored. This really depends on the
 * specific implementation.
 *
 * @tparam T the type of the elements stored on the queue. This type should be
 *           serializable.
 */
trait Queue[T] extends Enqueue[T] with Dequeue[T]

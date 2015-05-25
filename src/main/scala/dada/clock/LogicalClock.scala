package dada.clock

import dada.detail.TLogicalClock
import scala.language.implicitConversions
/**
 * Lamport Style Logical Clock.
 *
 * @author Angelo Corsaro <angelo@icorsaro.net>
 * @author Sara  Tucci    <sara@icorsaro.net>
 */

object LogicalClock {
  implicit def clock2TClock(clock: LogicalClock): TLogicalClock = new TLogicalClock(clock.ts, clock.id)
  implicit def TClock2Clock(clock: TLogicalClock): LogicalClock = new LogicalClock(clock.ts, clock.mid)

  val Infinite = new LogicalClock(Int.MaxValue, Int.MaxValue)
}

/**
 * This class provides an implementation of Lamport's Logical clocks.
 * Notice that a member-id is used in order enhance the basic logical clocks
 * with total-order -- essentially breaking ties by using the member-id.
 *
 * @param ts the  logical clock timestamp
 * @param id the member id
 *
 * @author <a href="mailto:angelo@icorsaro.net">Angelo Corsaro</a>
 * @author <a href="mailto:sara@icorsaro.net">Sara Tucci</a>

 */
case class LogicalClock(ts: Long, id: Int) extends Ordered[LogicalClock] {

  def compare(that: LogicalClock) = {
    val s1 = this.ts - that.ts
    if (s1 != 0) s1.toInt
    else this.id - that.id
  }

  /**
   * Increments the Logical clock by n steps
   *
   * @param n the number of steps by which the logical clock will be incremented
   * @return returns a new Logical Clock n-steps ahead of this.
   */
  def inc(n: Int): LogicalClock = new LogicalClock(ts + n, id)

  /**
   * Increments the Logical clock by n steps
   *
   * @param n the number of steps by which the logical clock will be incremented
   * @return returns a new Logical Clock n-steps ahead of this.
   */
  def +(n: Int): LogicalClock = inc(n)

  /**
   * Increments the Logical clock by n steps
   *
   * @return returns a new Logical Clock 1-step ahead of this.
   */
  def inc(): LogicalClock = inc(1)

  /**
   * Decrements the Logical clock by n steps
   *
   * @param n the number of steps by which the logical clock will be decremented
   * @return  returns a new Logical Clock n-steps behind of this.
   */
  def dec(n: Int): LogicalClock = new LogicalClock(ts - n, id)

  /**
   * Decrements the Logical clock by n steps
   *
   * @param n the number of steps by which the logical clock will be decremented
   * @return  returns a new Logical Clock n-steps behind of this.
   */
  def -(n: Int): LogicalClock = dec(n)

  /**
   * Decrements the Logical clock by 1 step
   *
   * @return returns a new Logical Clock 1-step behind of this.
   */
  def dec(): LogicalClock = dec(1)

  override def toString = "("+ ts +", "+ id +")"
}


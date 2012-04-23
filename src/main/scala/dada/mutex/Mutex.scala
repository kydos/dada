package dada.mutex

import dada.Config._

object Mutex {
  def apply(mid: Int, gid: Int) = new MutexType(mid, gid)
}

/**
 * This class represent a distributed mutex. Notice that distributed mutex are non-recursive.
 *
 * @author Angelo Corsaro <mailto:angelo@icorsaro.net>
 * @author Sara   Tucci   <mailto:sara@icorsaro.net>
 */
abstract class Mutex {
  /**
   * This operations tries to acquire a distributed Mutex and blocks up to when it is
   * not obtained.
   */
  def acquire()

  /**
   * This operation releases an acquired mutex.
   */
  def release()
}

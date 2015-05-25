package dada.concurrent

import scala.language.higherKinds
import java.util.concurrent.locks.{ReentrantReadWriteLock, ReentrantLock}
import java.util.concurrent.atomic.AtomicReference

package object synchronisers {

  final def compareAndSet[T](ref: AtomicReference[T])(fun: (T) => T): T = {
    var or: T = null.asInstanceOf[T]
    var cas = false
    do {
      val c = fun
      or = ref.get()
      cas = ref.compareAndSet(or, fun(or))
    } while (!cas)
    or
  }

  def syncrhonised[T](lock: ReentrantLock)(lambda: => T) = {
    lock.lock()
    try lambda finally lock.unlock()
  }

  def synchronisedRead[T](lock: ReentrantReadWriteLock)(lambda: => T) = {
    lock.readLock().lock()
    try lambda finally lock.readLock().unlock()
  }

  def synchronisedWrite[T](lock: ReentrantReadWriteLock)(lambda: => T) = {
    lock.writeLock().lock()
    try lambda finally lock.writeLock().unlock()
  }
}

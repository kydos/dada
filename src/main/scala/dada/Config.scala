package dada

object Config {
  type Logger = org.slf4j.Logger
  type LoggerFactory = org.slf4j.LoggerFactory
  implicit val dadaLogger = org.slf4j.LoggerFactory.getLogger("DLog");

  // Configuration of specific abstraction implementations
  type MutexType = mutex.LCMutex
  type QueueType[T] = queue.LCEventualQueue[T]
  type EnqueueType[T] = queue.LCEventualEnqueue[T]
}

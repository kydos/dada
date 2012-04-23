package dada.example

import dada.mutex.Mutex
import dada.group.EventualGroup
import dada.group.event.{MemberLeave, MemberJoin}
import java.util.concurrent.{Semaphore, CyclicBarrier}


object MutexExample {
  def main(args: Array[String]) {
    if (args.length < 3) {
      println(Console.GREEN + "USAGE:\n\t MutexExample <mid> <gid> <n>" + Console.RESET)
      sys.exit(1)
    }

    val mid = args(0).toInt
    val gid = args(1).toInt
    val n = args(2).toInt

    val group = EventualGroup(gid)
    group.join(mid)
    group.waitForViewSize(n)

    val mutex = Mutex(mid, gid)

    for (i <- 1 to 5) {
      println("[P" + mid + "]: Thinking...")
      var sleepTime = 4000 + (1000 * math.random).toInt
      Thread.sleep(sleepTime)
      mutex.acquire()
      println(Console.MAGENTA + Console.BLINK + "[P" + mid + "]: >>>>>>>>>>>>>>>>>>>>>>>>>>>>> Work Item: " + i + Console.RESET)
      println(Console.MAGENTA + Console.BLINK + "Type a character to Release Mutex:>> " + Console.RESET)
      readChar()
      mutex.release()
    }
    group.leave(mid)
  }
}

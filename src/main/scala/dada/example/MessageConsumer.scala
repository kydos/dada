package dada.example

import dada.queue._
import dada.group.Group
import dada.group.event.MemberJoin

object MessageConsumer {
  def main(args: Array[String]) {
    if (args.length < 4) {
      println("USAGE:\n\t MessageProducer <mid> <gid> <readers-num> <n>")
      sys.exit(1)
    }

    val mid = args(0).toInt
    val gid = args(1).toInt
    val rn = args(2).toInt
    val n = args(3).toInt

    println("Producer:> Joining Group")
    val group = Group(gid)
    group listen{
      case MemberJoin(id) => println("Joined M["+ id +"]")
    }

    group.join(mid)

    println("Producer:> Waiting for stable Group View")
    group.waitForViewSize(n)

    val queue = Queue[String]("CounterQueue", mid, gid, rn)

    val baseSleep = 1000
    while (true) {
      queue.sdequeue() match {
        case Some(s) => println(Console.MAGENTA_B + s + Console.RESET)
        case _ => println(Console.MAGENTA_B + "None" + Console.RESET)
      }
      val sleepTime = baseSleep + (math.random * baseSleep).toInt
      Thread.sleep(sleepTime)
    }
  }
}

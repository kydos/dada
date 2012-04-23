package dada.example

import dada.group.Group
import dada.queue.{Enqueue, Queue}
import dada.group.event.MemberJoin

object MessageProducer {
  def main(args: Array[String]) {
    val logger = dada.Config.dadaLogger
    if (args.length < 4) {
      println("USAGE:\n\t MessageProducer <mid> <gid> <n> <samples>")
      sys.exit(1)
    }

    val mid = args(0).toInt
    val gid = args(1).toInt
    val n = args(2).toInt
    val samples = args(3).toInt
    println("Producer:> Joining Group")
    val group = Group(gid)
    group.reactions += {
      case MemberJoin(mid) => println("Joined M["+ mid +"]")
    }
    group.join(mid)
    println("Producer:> Waiting for stable Group View")
    group.waitForViewSize(n)

    val queue = Enqueue[String]("CounterQueue", mid, gid)

    for (i <- 1 to samples) {
      val msg = "MSG["+ mid +", "+ i +"]"
      println(msg)
      queue.enqueue(msg)
      // Pace the write so that you can see what's going on
      Thread.sleep(300)
    }
  }

}

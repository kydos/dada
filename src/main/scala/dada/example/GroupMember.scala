package dada.example

import dada.group._
import event.{MemberLeave, MemberJoin, MemberFailure}


object GroupMember {
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("USAGE: GroupMember <gid> <mid>")
      sys.exit(1)
    }
    val gid = args(0).toInt
    val mid = args(1).toInt

    val group = Group(gid)

    group.join(mid)

    val printGroupView = () => {
      print("Group["+ gid +"] = { ")
      group.view foreach(m => print(m + " "))
      println("}")}

    group listen {
      case MemberFailure(id) => {
        println("Member "+ id + " Failed.")
        printGroupView()
      }
      case MemberJoin(id) => {
        println("Member "+ id + " Joined")
        printGroupView()
      }
      case MemberLeave(id) => {
        println("Member "+ id +" Left")
        printGroupView()
      }
    }
  Thread.currentThread().join()
  }
}

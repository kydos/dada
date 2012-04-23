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

    group.reactions += {
      case MemberFailure(mid) => {
        println("Member "+ mid + " Failed.")
        printGroupView()
      }
      case MemberJoin(mid) => {
        println("Member "+ mid + " Joined")
        printGroupView()
      }
      case MemberLeave(mid) => {
        println("Member "+ mid +" Left")
        printGroupView()
      }
    }

  }
}

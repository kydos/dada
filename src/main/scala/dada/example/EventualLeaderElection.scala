package dada.example

import dada.group.Group
import dada.group.event.{EpochChange, NewLeader, MemberFailure, MemberJoin}

object EventualLeaderElection {
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("USAGE: GroupMember <gid> <mid>")
      sys.exit(1)
    }
    val gid = args(0).toInt
    val mid = args(1).toInt

    val group = Group(gid)

    group.join(mid)

    group listen {
      case EpochChange(e) => {
        println(">> Epoch Changed to  "+ e)
        val lid = group.view.min
        group.proposeLeader(mid, lid)
        println(" >> Member "+ mid +" has voted for: "+ lid)
      }
      case MemberFailure(id) => println(">> MemberFailed = "+ id)
      case MemberJoin(id) => println(">> MemberJoin = "+ id)
      case NewLeader(l) => println(">> NewLeader  =  "+ l)
    }
  }
}

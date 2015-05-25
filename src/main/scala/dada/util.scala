package data

import org.omg.dds.sub.Sample
package object util {
  def show[T](s: Sample[T]): String = {
    val ss = s.getSampleState
    val is = s.getInstanceState
    val vs = s.getViewState
    // val gr = s.getGenerationRank
    val ih = s.getInstanceHandle.hashCode()
    s"sample ( ss = $ss, is = $is, vs = $vs, ih = $ih)"
  }
}

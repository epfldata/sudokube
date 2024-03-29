package examples

import core.solver.{Rational, RationalTools}
import core.solver.lpp.{Interval, SparseSolver}

object DataCubeOnlineAgg {
  def main(args: Array[String]): Unit = {

    import frontend.experiments.Tools._

    import RationalTools._

    class CB {
      var bounds: Option[collection.mutable.ArrayBuffer[Interval[Rational]]] =
        None

      def callback(s: SparseSolver[Rational]) = {
        println(s.bounds)
        bounds = Some(s.bounds)
        true
      }
    }

    val dc = mkDC(10, 1, 2, 10)
    val cb = new CB
    dc.online_agg[Rational](Vector(0, 1, 2), 2, cb.callback)
    val final_result = cb.bounds.get

  }
}

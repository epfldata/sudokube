//package ch.epfl.data.sudokube
package frontend
package experiments
import planning._
import core._
import combinatorics._
import util._
import backend._


object Tools {
  def qq(qsize: Int) = (0 to qsize - 1).toList

  def avg(n_it: Int, sample: () => Double) = {
    var a = 0.0
    for(i <- 1 to n_it) a += sample()
    a / n_it
  }

  class JailBrokenDataCube(
    m: MaterializationScheme,
    fc: Cuboid
  ) extends DataCube(m) with Serializable {
    build(fc)

    /// here one can access the cuboids directly
    def getCuboids = cuboids
  }

  def mkDC(n_bits: Int,
    rf: Double,
    base: Double,
    n_rows: Int,
    sampling_f: Int => Int = Sampling.f1,
    be: Backend[_] = CBackend.b
  ) = {
    val sch = schema.StaticSchema.mk(n_bits)
    val R   = sch.TupleGenerator(n_rows, sampling_f)
    println("mkDC: Creating maximum-granularity cuboid...")
    val fc  = be.mk(n_bits, R)
    println("...done")
    val m   = RandomizedMaterializationScheme(n_bits, rf, base)
    val dc = new DataCube(m); dc.build(fc)
//    val dc = new JailBrokenDataCube(m, fc)
//    assert(dc.getCuboids.last == fc)
    dc
  }
}


object minus1_adv {
  import java.io._

  /** the advantage of assembling the result from smaller projections over
      using only the full cube:
      how much bigger (in bits, i.e. log2(factor)) is the best cube to answer
      the query in full compared to the worst size cube
      of size |q| - 1?
  */
  def apply(n: Int, rf: Double, base: Double, qsize: Int, n_it: Int) = {
    val pw = new PrintWriter(new File("m1_adv_" + n + "_" + rf
      + "_" + base + "_" + qsize + "_" + n_it + ".csv" ))

    var accum_full_cost = 0

    for(i <- 1 to n_it) {
      val q = Tools.qq(qsize)
      val m = RandomizedMaterializationScheme(n, rf, base)

      val a = m.prepare(q, qsize - 1, n).groupBy(_.accessible_bits.length)
      val full_cost = a(q.length).head.mask.length
      accum_full_cost += full_cost

      a.get(q.length - 1) match {
        case Some(m1) => {
          val df = DF.compute_df0(q.length, m1.map(_.accessible_bits))

          val worst_proj_cost = m1.map(_.mask.length).max
          val   avg_proj_cost = m1.map(_.mask.length).sum.toDouble/m1.length
          val  best_proj_cost = m1.map(_.mask.length).min
          pw.write(n + "\t" + rf + "\t" + base + "\t" + qsize + "\t"
                + full_cost + "\t"
                + worst_proj_cost + "\t"
                + avg_proj_cost + "\t"
                + best_proj_cost + "\t"
                + df + "\n")
        }
        case None     => {
          // no improvement -- we need to use the full cube
          pw.write(n + "\t" + rf + "\t" + base + "\t" + qsize + "\t"
                + full_cost + "\t"
                + full_cost + "\t"
                + full_cost + "\t"
                + full_cost + "\t"
                + 0 + "\n")
        }
      }
      pw.flush
    }
    pw.close

    println(accum_full_cost.toDouble / n_it)
  }
}


/** estimates how much the storage will be under the FD model,
    for different rf and base parameters.
*/
object fd_storage {

  def apply(n: Int, qsize: Int, it: Int) = {
    import java.io._
    val pw = new PrintWriter(new File("fd_size_" + n + "_" + qsize
      + "_" + it + ".csv" ))

    var l = List[(Int, Int, Int)]()

    for(i <- 1 to 20; j <- 1 to 20) {
      val rf = i.toDouble/20             // 0.05 to 1
      val base = 1.0 + j.toDouble / 100  // 1.01 to 1.2 
      println(rf + " " + base)

      val m = core.RandomizedMaterializationScheme(n, rf, base)

      pw.write(rf + "\t" + base + "\t" + m.projections.length
        + "\t" + m.info.wc_ratio(30) + "\t" + m.info.wc_ratio(40)
        + "\t" + m.info.fd_ratio(30) + "\t" + m.info.fd_ratio(40) + "\n")
      pw.flush
    }
    pw.close
  }
}


/** Plot, for different typical exploratory query sizes, how the #degrees
    of freedom go down as we increase the maximum cuboid dimension to
    consider.
*/
object exp_e_df {
  /** I have tested it: df1 and df2 are the same. */
  protected def estimate_df(
    n_bits: Int, rf: Double, base: Double, qsize: Int,
    max_fetch_dim: Int
  ) = {
    import backend.Payload
    val m = RandomizedMaterializationScheme(n_bits, rf, base)
    val q = (0 to qsize-1).toList
    val l = m.prepare(q, max_fetch_dim, max_fetch_dim).map(_.accessible_bits)

/*
    val n_vval = l.map(x => Big.pow2(x.length)).sum
    val v = (1 to n_vval.toInt).map(x => new Payload(x, None)).toArray
    val bounds = SolverTools.mk_all_non_neg(1 << q.length)
    val df1 = Solver(q.length, bounds, l, v).df
*/
    val df2 = DF.compute_df0(qsize, l)
    // assert(df1 == df2)
    df2
  }

  def apply(n_bits: Int, rf: Double, base: Double, it: Int) {
    import java.io._
    val pw = new PrintWriter(new File("expedf_" + n_bits
      + "_" + rf +  "_" + base + "_" + it + ".csv" ))

    for(qsize <- 5 to 12; mfd5 <- 0 to 30) {
      val mfd = mfd5 * 5
      val av_df = (for(i <- 1 to it) yield
        estimate_df(n_bits, rf, base, qsize, mfd)).sum.toDouble/it

      pw.write(n_bits + "\t" + rf + "\t" + base + "\t" + it + "\t" + qsize + "\t" + mfd + "\t" + av_df + "\n")
      pw.flush
    }
    pw.close
  }
} // end object Exp_E_DF


object exp_error_bounds {
  /** for debugging */
  var last_dc : Option[DataCube] = None

  def apply(n_bits: Int, rf: Double, base: Double, n_rows: Int, qsize: Int,
            it: Int
  ) {
    import RationalTools._
    import java.io._
    val pw = new PrintWriter(new File("errb_" + n_bits
      + "_" + rf +  "_" + base + "_" + n_rows
      + "_" + qsize + "_" + it + ".csv"))

    val df_buf  = Util.mkAB[Double](20, _ => 0.0)
    val bou_buf = Util.mkAB[Double](20, _ => 0.0)

    for(i <- 1 to it) {

    val dc  = Tools.mkDC(n_bits, rf, base, n_rows) 
    last_dc = Some(dc)

    for(j <- 1 to 10) {
      var q = Util.rnd_choose(n_bits, qsize)

      var best_df = 1
      for(d5 <- 1 to 20) {
        val a = if(best_df > 0) {
          val d = d5 * 5
          println("\nIt: " + i + ":" + j + " Query = " + q + "; " + d + " dims")

          val s = dc.solver[Rational](q, d)
          s.compute_bounds

          best_df = s.df
          println("[!!] #solved=" + s.solved_vars.size
                  + " #df=" + s.df + " cum.span=" + s.cumulative_interval_span)

          val bou = s.bounds
          (bou.map {
            case Interval(Some(a), Some(b)) => (b-a).toDouble / 2
            case _ => 1000.0
          }).sum / bou.length
        }
        else 0.0

        df_buf(d5-1)  += best_df
        bou_buf(d5-1) += a
      }
    } }

  for(d5 <- 1 to 20) {
    val d = d5 * 5
    pw.write(d + "\t" + df_buf(d5-1)/it/10 + "\t" + bou_buf(d5-1)/it/10 + "\n")
  }
  pw.close
  }
}


/*
// for checking that the intervals get monotonically more narrow
// as one increases dimensionality.

import core._
import RationalTools._
import frontend._
import frontend.experiments._

val n_bits = 60
val dc     = Tools.mkDC(n_bits, .2, 1.15, 100, Sampling.f1)
val qsize  = 6
var q      = util.Util.rnd_choose(n_bits, qsize)

var prev_span: Option[Rational]        = None
var prev_df:   Option[List[List[Int]]] = None

for(d <- (1 to 12).map(_ * 2)) {
  val s = dc.solver[Rational](q, d)

  if(Some(s.df) == prev_df) println("Nothing to do.")
  else {
    s.compute_bounds

    val solved = s.solved_vars.size
    val cumu   = s.cumulative_interval_span

    println("\n" + (solved, s.df, "%5.1f".format(
      cumu.get.toDouble / (1 << qsize))))

    (prev_span, cumu) match {
      case (Some(x), Some(y)) => assert(x >= y)
      case _ => {}
    }
    prev_span = cumu
    //prev_l = Some(s.df)
  }
}

*/



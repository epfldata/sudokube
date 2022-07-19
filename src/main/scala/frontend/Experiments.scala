//package ch.epfl.data.sudokube
package frontend
package experiments
import planning._
import core._
import combinatorics._
import util._
import backend._
import core.cube.ArrayCuboidIndexFactory
import core.materialization.{MaterializationScheme, MaterializationSchemeInfo, OldRandomizedMaterializationScheme}
import core.solver.{Rational, RationalTools, SolverTools}
import core.solver.lpp.{Interval, SliceSparseSolver}
import generators._

import scala.util.Random
object Tools {
  def round(v: Double, digits: Int) ={
    val prec = math.pow(10, digits)
    math.floor(v * prec)/prec
  }
  //For Randomized Materialization Scheme so that a specific column has 1 cuboid and 4th level has 10^4 cuboids
  def params(nbits: Int, colWith1: Int) = {
    /*
      x * y^(n-4) = 10^4
      x * y^(n-c1) = 1

      logy = (c1-4)/4
      logx = -(n-c1)  logy
     */
    val logy = 4.0/(colWith1-4)
    val logx = -(nbits-colWith1) * logy
    (logx, logy)
  }

  def qq(qsize: Int) = (0 to qsize - 1)
  def rand_q(n: Int, qsize: Int) = Random.shuffle((0 until n).toVector).take(qsize).sorted

  def avg(n_it: Int, sample: () => Double) = {
    var a = 0.0
    for(i <- 1 to n_it) a += sample()
    a / n_it
  }

  class JailBrokenDataCube(
    m: MaterializationScheme,
    fc: Cuboid
  ) extends DataCube with Serializable {
    build(fc, m)

    /// here one can access the cuboids directly
    def getCuboids = cuboids
  }

  def mkDC(n_bits: Int,
           rf: Double,
           base: Double,
           n_rows: Long,
           sampling_f: Int => Int = Sampling.f1,
           be: Backend[_] = CBackend.b,
           vg: ValueGenerator = RandomValueGenerator(10)
          ) = {
    val sch = schema.StaticSchema.mk(n_bits)
    val R = TupleGenerator(sch, n_rows, sampling_f)
    println("mkDC: Creating maximum-granularity cuboid...")
    val fc = Profiler("Full Cube"){be.mk(n_bits, R)}
    println("...done")
    val m = OldRandomizedMaterializationScheme(n_bits, rf, base)
    val dc = new DataCube();
    Profiler("Projections"){dc.build(fc, m)}
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
      of (projected) size |q| - 1?
  */
  def apply(nbits: Int, rf: Double, base: Double, lognrows: Int, qsize: Int, num_it: Int) = {
    val pw = new PrintWriter(new File("expdata/m1_adv_" + nbits + "_" + rf
      + "_" + base + "_" + lognrows + "_" + qsize + "_" + num_it + ".csv"))
    pw.println(s"nbits,rf,base,lognrows,qsize,full_cost,worst_proj_cost,avg_proj_cost,best_proj_cost,sum_proj_cost," +
    "gauss_cost,expected_solver_cost,#proj,#df")
    def cost(bits: Int) = bits //Math.min(lognrows, bits)
    var accum_full_cost = 0

    for(i <- 1 to num_it) {
      val q = Tools.qq(qsize)
      val m = OldRandomizedMaterializationScheme(nbits, rf, base)

      //SBJ: Changed parameters for this calculation
      val a = ArrayCuboidIndexFactory.buildFrom(m).prepareBatch(q).groupBy(ps => Bits.sizeOfSet(ps.queryIntersection))
      val full_cost = cost(a(q.length).head.cuboidCost)
      accum_full_cost += full_cost

      a.get(q.length - 1) match {
        case Some(m1) => {
          val df = DF.compute_df0(q.length, m1.map(_.queryIntersection))
          val detsize = (1 << q.length) - df

          val worst_proj_cost = cost(m1.map(_.cuboidCost).max)
          val avg_proj_cost = cost((m1.map(_.cuboidCost).sum.toDouble/m1.length).toInt)
          val sum_proj_cost = cost((avg_proj_cost + Math.log(m1.length)/Math.log(2)).toInt)
          val  best_proj_cost = cost(m1.map(_.cuboidCost).min)

         //val n = Math.log(df.toDouble)/Math.log(2)
          val m = Math.log(detsize.toDouble)/Math.log(2)
          val v = 0  //V is the avg non-zero entry per row. Best case : 1 , Worst Case N
          val gauss_cost = 2*m + v //log cost of Gaussing elimination log(M^2 V) .

          val expected_cost = Math.max(sum_proj_cost, gauss_cost)
          pw.write(nbits + "," + rf + "," + base + "," + lognrows + "," + qsize + ","
                + full_cost + ","
                + worst_proj_cost + ","
                + avg_proj_cost + ","
                + best_proj_cost + ","
                + sum_proj_cost + ","
                + gauss_cost + ","
                + expected_cost + ","
                + m1.length + ","
                + df + "\n")
        }
        case None     => {
          // no improvement -- we need to use the full cube
          pw.write(nbits + "," + rf + "," + base + "," + lognrows + "," + qsize + ","
                + full_cost + ","
                + full_cost + ","
                + full_cost + ","
                + full_cost + ","
                + full_cost + ","
                + 0 + ","
                + full_cost + ","
                + 1 + ","
                + 0 + "\n")
        }
      }
      pw.flush
    }
    pw.close

    println(accum_full_cost.toDouble / num_it)
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

      val m = OldRandomizedMaterializationScheme(n, rf, base)
      val info = new MaterializationSchemeInfo(m)
      pw.write(rf + "\t" + base + "\t" + m.projections.length
        + "\t" + info.wc_ratio(30) + "\t" + info.wc_ratio(40)
        + "\t" + info.fd_ratio(30) + "\t" + info.fd_ratio(40) + "\n")
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
    val m = OldRandomizedMaterializationScheme(n_bits, rf, base)
    val q = (0 to qsize-1)
    val l = ArrayCuboidIndexFactory.buildFrom(m).prepareBatch(q, max_fetch_dim).map(_.queryIntersection)

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
      var q = Util.rnd_choose(n_bits, qsize).toIndexedSeq

      var best_df = 1
      for(d5 <- 1 to 20) {
        val a = if(best_df > 0) {
          val d = d5 * 5
          println("\nIt: " + i + ":" + j + " Query = " + q + "; " + d + " dims")

          //TODO: SBJ: The parameter to dc.solver is in terms of original cuboid size. Check if d here is the size before or after projection
          val l = Profiler("SolverPrepare") { dc.index.prepareBatch(q, d) }
          val bounds = SolverTools.mk_all_non_neg[Rational](1 << q.length)
          val data = Profiler("SolverFetch") { dc.fetch2(l) }
          val s = Profiler("SolverConstructor") { new SliceSparseSolver[Rational](q.length, bounds, l.map(_.queryIntersection), data) }


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



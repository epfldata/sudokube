package experiments

import core.{DataCube, Rational, SolverTools, SparseSolver}
import frontend._
import frontend.experiments._
import planning.ProjectionMetaData
import util.{AutoStatsGatherer, Profiler, StatsGatherer}

import java.io.PrintStream

object CubeData {



  def mkCube(log: Int) = {
    Profiler.resetAll()
    n_row_log = log
    val name = s"${sampling}_d${n_bits}_${rf}_${base}_n${n_row_log}"
    val nrows = 1L << n_row_log

    val sampling_f = sampling match {
      case "S1" => Sampling.f1(_)
      case "S2" => Sampling.f2(_)
    }

     dc = Profiler("mkDC") {
      Tools.mkDC(n_bits, rf, base, nrows, sampling_f)
    }
    Profiler("Save DC") {
      dc.save(name)
    }
    Profiler.print()
  }

  val n_bits = 100
  val rf = 0.1
  val base = 1.05
  //(0 to 4).map( i => 20 + i * 5).foreach { n =>
  //  val dcw = Tools.mkDC(n_bits, rf, base,  1L << n, Sampling.f1)
  //}

  var dc: DataCube = null
  var n_row_log = 0
  var sampling = "S2"

  def loadDC(log: Int): Unit = {
    n_row_log = log
    val cubename = s"${sampling}_d${n_bits}_${rf}_${base}_n${n_row_log}"
    dc = Profiler("DC Load") {
      core.DataCube.load(cubename)
    }
    Profiler.print()
  }

  def expt(qsizes: Seq[Int], iters: Int) {
    import core.RationalTools._
    val pw = new PrintStream(s"expdata/expt_${sampling}_${n_bits}_${rf}_${base}_${n_row_log}_timedata.csv")
    val pwstat = new PrintStream(s"expdata/expt_${sampling}_${n_bits}_${rf}_${base}_${n_row_log}_statdata.csv")
    val header = s"NBits,rf,Base,logNrows,Qsize,IterNum, NaiveTotalTime(ms), NaiveMaxDimFetched, NaiveInit(ms), NaiveFetch(ms), SolverTotalTime(ms),SolverMaxDimFetched, SolverRounds, SolverInitTime(ms),SolverFetch(ms),SolverAdd(ms),SolverGauss(ms),SolverComputeBounds(ms)"
    val headerStat = s"NBits,rf,Base,logNrows,Qsize,IterNum, TimeElapsed(ms),#df,#solved,CumIntSpan"
    pw.println(header)
    pwstat.println(headerStat)

    qsizes.foreach { qsize =>
      val cheap_size = 2 * qsize

      (0 until iters).foreach { iternum =>

        Profiler.resetAll()
        var naiveDimFetched = 0
        var solverDimFetched = 0
        var solverRounds = 0

        val query = Tools.rand_q(n_bits, qsize)


        val naiveRes = Profiler("Naive") {
          val naivePlan = Profiler("NaiveInit") {
            dc.m.prepare(query, n_bits, n_bits)
          }
          naiveDimFetched = naivePlan.head.mask.length
          Profiler("NaiveFetch") {
            dc.fetch2(naivePlan)
          }
        }

        val stg = Profiler("Solver") {
          var l = Profiler("Init") {
            dc.m.prepare_online_agg(query, cheap_size)
          }
          val bounds = Profiler("Init") {
            SolverTools.mk_all_non_neg[Rational](1 << query.length)
          }
          val s = Profiler("Init") {
            SparseSolver[Rational](query.length, bounds, Nil, Nil, _ => true)
          }
          var df = s.df

          val statsGatherer = new AutoStatsGatherer(s.getStats)
          statsGatherer.start()

          while (!(l.isEmpty) && (df > 0)) {

            if (s.shouldFetch(l.head.accessible_bits)) {
              solverRounds += 1
              if (l.head.mask.length > solverDimFetched)
                solverDimFetched = l.head.mask.length

              println(l.head.accessible_bits)
              val fetched = Profiler("Fetch") {
                dc.fetch2(List(l.head))
              }
              Profiler("SolverAdd") {
                s.add2(List(l.head.accessible_bits), fetched)
              }
              //TODO: Probably gauss not required if newly added variables are first rewritten in terms of non-basic

                Profiler("Gauss") {
                  s.gauss(s.det_vars)
                }
                Profiler("ComputeBounds") {
                  s.compute_bounds
                }
                df = s.df

            } else {
              println(s"Preemptively skipping fetch of cuboid ${l.head.accessible_bits}")
            }
            l = l.tail
            Profiler.print()
          }
          println("Remaining cuboids = "+l.size)
          statsGatherer.finish()
          statsGatherer
        }

        val result = s"$n_bits,$rf,$base,$n_row_log,${qsize},${iternum},  " +
          Profiler.durations("Naive")._2 / (1000 * 1000) + "," + naiveDimFetched + "," +
          Profiler.durations("NaiveInit")._2 / (1000 * 1000) + "," +
          Profiler.durations("NaiveFetch")._2 / (1000 * 1000) + ",  " +
          Profiler.durations("Solver")._2 / (1000 * 1000) + "," + solverDimFetched + "," + solverRounds + "," +
          Profiler.durations("Init")._2 / (1000 * 1000) + "," +
          Profiler.durations("Fetch")._2 / (1000 * 1000) + "," +
          Profiler.durations("SolverAdd")._2 / (1000 * 1000) + "," +
          Profiler.durations("Gauss")._2 / (1000 * 1000) + "," +
          Profiler.durations("ComputeBounds")._2 / (1000 * 1000)
        println("\n\n\n" + result)
        pw.println(result)

        stg.stats.map{kv =>
         val statres = s"$n_bits,$rf,$base,$n_row_log,${qsize},${iternum},  " +
            s"${kv._1}, ${kv._3._1}, ${kv._3._2}, ${kv._3._3}"
          println(statres)
          pwstat.println(statres)
        }

        println("\n\n\n")


      }
    }
    pw.close()
    pwstat.close()
    //Profiler.print()
  }

  import util.Profiler
  import scala.util.Random

  def rnd(n: Int, base: Int) = {
    Profiler.resetAll()
    var sum = 0
    Profiler("Random") {
      (0 until (1 << n)).foreach(i => sum += Random.nextInt(1 << base))
    }
    Profiler.print()
    println(sum)
  }

  def func(nbits: Int, lognrows: Int) = {
    val nrows = 1 << lognrows
    val rows = scala.collection.mutable.ArrayBuffer[BigInt]()
    Profiler.resetAll()
    (0 until nrows).foreach { i =>
      val mysum = Profiler("FUNC") {
        (0 until nbits).foldLeft((BigInt(0), BigInt(1))) { case ((sum, prod), cur) =>
          Sampling.f1(2) match {
            case 0 => (sum, prod << 1)
            case 1 => (sum + prod, prod << 1)
          }
        }._1
      }
      rows += mysum
    }

    Profiler.print()
  }

  def main(args: Array[String]) {
    import core.RationalTools._
    //n_row_log = 30
    //(1 to 10).foreach { i => minus1_adv(n_bits, rf, base, n_row_log, i, 100)}
    //mkCube(15)
    //loadDC(20)
    //loadDC(20)
    //loadDC(6)
    //println("Zeros naive = " + dc.naive_eval(List(0,1,2,3)).zipWithIndex.filter(_._1 == 0).mkString(" "))
    //expt(10 to 10, 1)
    //(1 to 10).foreach{i => expt(i, 1)}
    //System.out.flush()
    //System.err.flush()
    //Profiler.print()

    mkCube(15)
    //loadDC(15)
    val q = List(63, 62, 61, 60).sorted
    val s = dc.solver[Rational](q, 3)
    s.compute_bounds
    println("DF = "+s.df)
    s.bounds.foreach(println)
    println(dc.naive_eval(q).mkString("   "))

  }
}

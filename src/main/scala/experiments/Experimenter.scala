package experiments

import backend.CBackend
import combinatorics.Combinatorics
import core._
import core.solver.Strategy._
import core.solver._
import frontend.experiments.Tools
import frontend.generators.{MicroBench, NYC, SSB}
import frontend.schema.encoders._
import util.{Bits, Profiler, Util}

import java.io.PrintStream
import scala.reflect.ClassTag
import scala.util.Random

object Experimenter {

  def schemas(): Unit = {
    List(NYC, SSB(100)).foreach { cg =>
      val sch = cg.schema()
      println(cg.inputname)
      sch.columnVector.map(c => c.name + ", " + c.encoder.bits.size).foreach(println)
      println("\n\n")
    }
  }

  def cuboid_distribution(isSMS: Boolean) = {
    val ms = if (isSMS) "sms3" else "rms3"
    val cg = NYC
    val maxD = 30
    val nycmaxD = 30
    val cubes = List(
      s"NYC_${ms}_13_10_$nycmaxD",
      s"NYC_${ms}_15_6_$nycmaxD",
      s"NYC_${ms}_15_10_$nycmaxD",
      s"NYC_${ms}_15_14_$nycmaxD",
      s"NYC_${ms}_17_10_$nycmaxD"
    )

    val fileout = new PrintStream(s"expdata/Cuboids_${cg.inputname}_${ms}.csv")
    fileout.println("Name," + (0 to maxD).mkString(","))
    cubes.foreach { n =>
      val names = n.split("_")
      println(s"Getting cuboid distribution for $n")
      val logN = names(2).toInt
      val minD = names(3).toInt
      val dc = PartialDataCube.load2(n, cg.inputname + "_base")
      val projMap = dc.m.projections.groupBy(_.length).mapValues(_.length).withDefaultValue(0)
      val projs = (0 to maxD).map(i => projMap(i)).mkString(",")
      fileout.println(s"${logN}_${minD}," + projs)
      dc.cuboids.head.backend.reset
    }
    val sch = cg.schema()
    val total = if (isSMS) {
      sch.root.numPrefixUpto(maxD).map(_.toDouble).toList
    }
    else {
      (0 to maxD).map { i => Combinatorics.comb(sch.n_bits, i).toDouble }.toList
    }
    fileout.println(s"Total," + total.mkString(","))
  }

  def storage_overhead() = {
    def split(s: String) = "\\begin{tabular}{c}" + s + "\\end{tabular}"

    val fileout = new PrintStream(s"expdata/storage_overhead.tex")

    fileout.println(
      """
        |\begin{table}
        |\caption{Additional Storage Overhead}
        |\label{tab:overhead}
        |\begin{tabular}{|c|c|c|c|c|c|}
        |\hline
        |""".stripMargin +
        "Dataset & " + split("Base \\\\ Size") + "& $n$ & " + "$d_{\\min}$ & " + split("RMS \\\\ Overhead.") + "&" + split("SMS \\\\ Overhead.") + " \\\\ \n \\hline \n")
    val maxDNYC = 30
    val maxDSSB = 30
    val params = List(
      "NYC" -> s"13_10_$maxDNYC",
      "NYC" -> s"15_6_$maxDNYC",
      "NYC" -> s"15_10_$maxDNYC",
      "NYC" -> s"15_14_$maxDNYC",
      "NYC" -> s"17_10_$maxDNYC",
      "SSB-sf100" -> s"15_14_$maxDSSB")
    params.foreach { case (cgname, cubename) =>
      val names = cubename.split("_")
      val logN = names(0).toInt
      val minD = names(1).toInt
      val rmsname = cgname + "_rms3_" + cubename
      val smsname = cgname + "_sms3_" + cubename

      println(s"Getting storage overhead for $rmsname")
      val dcrms = PartialDataCube.load2(rmsname, cgname + "_base")
      val basesize = dcrms.cuboids.last.numBytes
      val baseGB = Tools.round(basesize / math.pow(10, 9), 2)
      val rmsovrhead = Tools.round(dcrms.cuboids.map(_.numBytes).sum / basesize.toDouble - 1.0, 4)
      dcrms.cuboids.head.backend.reset

      println(s"Getting storage overhead for $smsname")
      val dcsms = PartialDataCube.load2(smsname, cgname + "_base")
      val smsvrhead = Tools.round(dcsms.cuboids.map(_.numBytes).sum / basesize.toDouble - 1.0, 4)
      dcsms.cuboids.head.backend.reset

      val (ds, bs) = if (cgname.startsWith("NYC")) {
        if (cubename.startsWith("13"))
          ("\\multirow{5}{*}{NYC}", s"\\multirow{5}{*}{$baseGB GB}")
        else ("", "")
      }
      else ("\\hline\nSSB", s"$baseGB GB")


      fileout.println(s"$ds & $bs & " + "$2^{" + logN + "}$" + s" & $minD & $rmsovrhead & $smsvrhead \\\\")

    }
    fileout.println(
      """\hline
        |\end{tabular}
        |\end{table}
        |""".stripMargin)
  }

  def lpp_query_dimensionality(isSMS: Boolean)(implicit shouldRecord: Boolean, numIters: Int) = {
    val cg = SSB(100)

    val param = "15_14_30"
    val ms = (if (isSMS) "sms3" else "rms3")
    val name = s"_${ms}_${param}"
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load2(fullname, cg.inputname + "_base")
    val sch = cg.schema()


    val expname2 = s"query-dim-$ms"

    import RationalTools._
    val expt = new LPSolverBatchExpt[Rational](expname2)
    if (shouldRecord) expt.warmup()
    val qss = List(6, 8, 10, 12)
    qss.foreach { qs =>
      println(s"LP Solver Experiment for MS = $ms Query Dimensionality = $qs")
      val queries = (0 until numIters).map(_ => sch.root.samplePrefix(qs)).distinct
      val ql = queries.length
      queries.zipWithIndex.foreach { case (q, i) =>
        println(s"Batch Query $i/$ql")
        expt.run(dc, fullname, q, null, true, sliceValues = Vector())
      }
    }


    dc.cuboids.head.backend.reset
  }


  def momentCompareFixedSlice()(implicit shouldRecord: Boolean, numIters: Int): Unit = {
    val cg = SSB(100)
    val param = "15_14_30"
    val ms = "sms3"
    val name = s"_${ms}_${param}"
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load2(fullname, cg.inputname + "_base")
    dc.loadPrimaryMoments(cg.inputname + "_base")

    val mq = new MaterializedQueryResult(cg)
    val expt = new MomentSolverCompareBatchExpt("fixedslice")
    //if (shouldRecord) expt.warmup()  //warmup has only 6 bits
    val qss = List(9, 12, 15, 18, 21, 24)
    qss.foreach { qs =>
      println(s"\n\nMoment Solver Strategy Comparison Experiment Fixed Slice for MS = $ms Query Dimensionality = $qs")
      val queries = mq.loadQueries(qs).take(numIters)
      val ql = queries.length
      queries.zipWithIndex.foreach { case (q, i) =>
        println(s" Query ${i + 1}/$ql")
        val trueResult = mq.loadQueryResult(qs, i)
        expt.run(dc, fullname, q, trueResult, true, sliceValues = Vector(1, 0, 0, 0, 1, 1, 0, 0))
      }
    }

  }

  def momentCompareFixedTotal()(implicit shouldRecord: Boolean, numIters: Int): Unit = {
    val cg = SSB(100)
    val param = "15_14_30"
    val ms = "sms3"
    val name = s"_${ms}_${param}"
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load2(fullname, cg.inputname + "_base")
    dc.loadPrimaryMoments(cg.inputname + "_base")

    val mq = new MaterializedQueryResult(cg)
    val expt = new MomentSolverCompareBatchExpt("fixedtotal")
    //if (shouldRecord) expt.warmup()  //warmup has only 6 bits
    val sss = List(0, 4, 8, 12, 16, 20)
    //val sss = List( 18, 21)
    val qs = 24
    val queries = mq.loadQueries(qs).take(numIters)
    val ql = queries.length
    println(s"\n\nMoment Solver Strategy Comparison Fixed Total Experiment for MS = $ms")
    queries.zipWithIndex.foreach { case (q, i) =>
      println(s" Query ${i + 1}/$ql")
      val trueResult = mq.loadQueryResult(qs, i)
      sss.foreach { ss =>
        val svs = if(ss == 0) Vector()  else {
          Bits.intToMask(ss, Random.nextInt(1 << ss))
        }
        println(s" Slice Dimensionality = $ss  slice=${svs.mkString(":")}")
        expt.run(dc, fullname, q, trueResult, true, sliceValues = svs)
      }
    }


  }

  def trieExpt[T: ClassTag : Fractional]()(implicit shouldRecord: Boolean, numIters: Int): Unit = {

    val cg = SSB(100)
    val param = "15_14_30"
    val ms = "sms3"
    val name = s"_${ms}_${param}"
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load2(fullname, cg.inputname + "_base")
    dc.loadPrimaryMoments(cg.inputname + "_base")
    //val trie = dc.loadTrie(fullname)
    val trie_filename = s"cubedata/${fullname}_trie/${fullname}.ctrie"
    CBackend.b.loadTrie(trie_filename)
    val sch = cg.schema()

    def momentSolve(q: List[Int]) = {
      val (l, pm) = Profiler("Moment Prepare") {
        dc.m.prepare(q, dc.m.n_bits - 1, dc.m.n_bits - 1) -> SolverTools.preparePrimaryMomentsForQuery[T](q, dc.primaryMoments)
      }
      val maxDimFetch = l.last.mask.length
      //println("Solver Prepare Over.  #Cuboids = "+l.size + "  maxDim="+maxDimFetch)
      val fetched = Profiler("Moment Fetch") {
        l.map { pm =>
          (pm.accessible_bits, dc.fetch2[T](List(pm)).toArray)
        }
      }
      val result = Profiler(s"Moment Solve") {
        val s = Profiler(s"Moment Constructor") {
          new CoMoment4Solver[T](q.length, true, Moment1Transformer(), pm)
        }
        Profiler(s"Moment Add") {
          fetched.foreach { case (bits, array) => s.add(bits, array) }
        }
        println("Moments Add")
        s.momentsToAdd.foreach(println)
        Profiler(s"Moment FillMissing") {
          s.fillMissing()
        }
        Profiler(s"Moment ReverseTransform") {
          s.solve()
        }
        s.moments
      }
      result
    }

    def trieSolve(q: List[Int]) = {
      val pm = Profiler("Trie Prepare pm") {
        SolverTools.preparePrimaryMomentsForQuery(q, dc.primaryMoments)
      }
      val moments = Profiler("Trie Moments") {
        CBackend.b.prepareFromTrie(q)
      }
      val result2 = Profiler(s"Trie Solve") {
        val s = Profiler(s"TrieMoment Constructor") {
          new CoMoment4Solver(q.length, true, Moment1Transformer(), pm)
        }
        println("Trie prepare")
        Profiler("Trie Moments Add") {
          s.momentsToAdd ++= moments.map { case (i, l) =>
            s.knownSet += i
            println(i -> l)
            i -> Util.fromLong(l)
          }
        }
        Profiler(s"TrieMoment FillMissing") {
          s.fillMissing()
        }
        Profiler(s"TrieMoment ReverseTransform") {
          s.solve()
        }
        s.moments
      }
      result2
    }

    val expname2 = s"query-dim-$ms"
    val mq = new MaterializedQueryResult(cg)
    val qss = List(7)
    qss.foreach { qs =>
      Profiler.resetAll()
      //val queries = mq.loadQueries(qs).take(1)
      val queries = List(List(165, 181, 182, 183, 184, 185, 186))
      println(s"Moment Solver  Trie Experiment for MS = $ms Query Dimensionality = $qs")
      val ql = queries.length

      queries.zipWithIndex.foreach { case (q, i) =>
        println(s"Query ${i + 1}/$ql =  ${q.mkString(":")}")
        //val trueResult = mq.loadQueryResult(qs, i)
        Profiler.resetAll()
        val result1 = Profiler("MomentTotal") {
          momentSolve(q)
        }
        import SolverTools.error
        //val err1 = error(trueResult, result1)
        Profiler.print()
        //println("Error1 = " + err1)
        println("\n")
        Profiler.resetAll()
        val result2 = Profiler("TrieTotal") {
          trieSolve(q)
        }
        val num = implicitly[Fractional[T]]
        result1.zip(result2).zipWithIndex.foreach { case ((r1, r2), i) => if (!num.equiv(r1, r2)) println(s"R1[$i]=$r1 != $r2=R2[$i]") }
        assert(result1.sameElements(result2))
        //val err2 = error(trueResult, result2)
        Profiler.print()
        //println("Error2 = " + err2)
        println("\n\n\n")
      }
    }
    dc.cuboids.head.backend.reset
  }

  def moment_query_dimensionality(strategy: Strategy, isSMS: Boolean)(implicit shouldRecord: Boolean, numIters: Int): Unit = {

    val cg = SSB(100)
    val param = "15_14_30"
    val ms = (if (isSMS) "sms3" else "rms3")
    val name = s"_${ms}_${param}"
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load2(fullname, cg.inputname + "_base")
    dc.loadPrimaryMoments(cg.inputname + "_base")
    val sch = cg.schema()

    val expname2 = s"query-dim-$ms"
    val exptfull = new NewMomentSolverBatchExpt(strategy, expname2)
    if (shouldRecord) exptfull.warmup()

    val exptonline = new NewMomentSolverOnlineExpt(strategy, expname2)
    if (shouldRecord) exptonline.warmup()

    val qss = List(6, 9, 12, 15)
    qss.foreach { qs =>
      val queries = (0 until numIters).map(_ => sch.root.samplePrefix(qs)).distinct
      println(s"Moment Solver Experiment for MS = $ms Query Dimensionality = $qs")
      val ql = queries.length
      queries.zipWithIndex.foreach { case (q, i) =>
        println(s"Batch Query ${i + 1}/$ql")
        exptfull.run(dc, fullname, q, null, sliceValues = Vector())
      }

      queries.zipWithIndex.foreach { case (q, i) =>
        println(s"Online Query ${i + 1}/$ql")
        exptonline.run(dc, fullname, q, null, sliceValues = Vector())
      }
    }
    dc.cuboids.head.backend.reset
  }

  def moment_mat_params(strategy: Strategy, isSMS: Boolean)(implicit shouldRecord: Boolean, numIters: Int) = {
    val cg = NYC
    val params = List(
      (13, 10),
      (15, 6), (15, 10), (15, 14),
      (17, 10)
    )
    val sch = cg.schema()
    val maxD = 30
    val qs = 10
    val queries = (0 until numIters).map(_ => sch.root.samplePrefix(qs)).distinct
    val ms = (if (isSMS) "sms3" else "rms3")
    val expname2 = s"mat-params-$ms"
    val exptfull = new NewMomentSolverBatchExpt(strategy, expname2)
    if (shouldRecord) exptfull.warmup()

    val exptonline = new NewMomentSolverOnlineExpt(strategy, expname2)
    if (shouldRecord) exptonline.warmup()

    params.foreach { p =>
      val fullname = s"${cg.inputname}_${ms}_${p._1}_${p._2}_$maxD"
      val dc = PartialDataCube.load2(fullname, cg.inputname + "_base")
      dc.loadPrimaryMoments(cg.inputname + "_base")
      println(s"Moment Solver Materialization Parameters Experiment for $fullname")
      val ql = queries.length
      queries.zipWithIndex.foreach { case (q, i) =>
        println(s"Batch Query ${i + 1}/$ql")
        exptfull.run(dc, fullname, q, null, sliceValues = Vector())
      }

      queries.zipWithIndex.foreach { case (q, i) =>
        println(s"Online Query ${i + 1}/$ql")
        exptonline.run(dc, fullname, q, null, sliceValues = Vector())
      }

      dc.cuboids.head.backend.reset
    }
  }

  def mb_dims()(implicit shouldRecord: Boolean, numIters: Int): Unit = {
    val expt = new NewMomentSolverOnlineExpt(CoMoment3, "mb-dims", true)
    if (shouldRecord) expt.warmup()

    List(6, 8, 10, 12).foreach { nb =>
      println("Microbenchmark for Dimensionality = " + nb)
      val cg = MicroBench(nb, 100000, 0.5, 0.25)
      val fullname = cg.inputname + "_all"

      (1 to numIters).foreach { i =>
        println(s"Trial $i/$numIters")
        val (sch, r_its) = cg.generate2()
        sch.initBeforeEncode()
        val dc = new DataCube(MaterializationScheme.all_cuboids(cg.n_bits))
        dc.build(CBackend.b.mkParallel(sch.n_bits, r_its))
        dc.primaryMoments = SolverTools.primaryMoments(dc, false)
        val q = 0 until cg.n_bits
        expt.run(dc, fullname, q, null, sliceValues = Vector())
        dc.cuboids.head.backend.reset
      }
    }
  }


  def mb_total()(implicit shouldRecord: Boolean, numIters: Int): Unit = {
    val expt = new NewMomentSolverOnlineExpt(CoMoment3, "mb-total", true)
    if (shouldRecord) expt.warmup()
    List(2, 3, 4, 5).foreach { tot =>
      println("Microbenchmark Total = 10^" + tot)
      val cg = MicroBench(10, math.pow(10, tot).toInt, 0.5, 0.25)
      val fullname = cg.inputname + "_all"
      (1 to numIters).foreach { i =>
        println(s"Trial $i/$numIters")
        val (sch, r_its) = cg.generate2()
        sch.initBeforeEncode()
        val dc = new DataCube(MaterializationScheme.all_cuboids(cg.n_bits))
        dc.build(CBackend.b.mkParallel(sch.n_bits, r_its))
        dc.primaryMoments = SolverTools.primaryMoments(dc, false)

        val q = 0 until cg.n_bits
        expt.run(dc, fullname, q, null, sliceValues = Vector())
        dc.cuboids.head.backend.reset
      }
    }
  }


  def mb_stddev()(implicit shouldRecord: Boolean, numIters: Int): Unit = {
    val expt = new NewMomentSolverOnlineExpt(CoMoment3, "mb-stddev", true)
    if (shouldRecord) expt.warmup()
    List(0.2, 0.4, 0.6, 0.8).foreach { stddev =>
      println("Microbenchmark for stddev = " + stddev)
      val cg = MicroBench(10, 100000, stddev, 0.25)
      val fullname = cg.inputname + "_all"
      (1 to numIters).foreach { i =>
        println(s"Trial $i/$numIters")
        val (sch, r_its) = cg.generate2()
        sch.initBeforeEncode()
        val dc = new DataCube(MaterializationScheme.all_cuboids(cg.n_bits))
        dc.build(CBackend.b.mkParallel(sch.n_bits, r_its))
        dc.primaryMoments = SolverTools.primaryMoments(dc, false)

        val q = 0 until cg.n_bits
        expt.run(dc, fullname, q, null, sliceValues = Vector())
        dc.cuboids.head.backend.reset
      }
    }
  }

  def mb_prob()(implicit shouldRecord: Boolean, numIters: Int): Unit = {
    val expt = new NewMomentSolverOnlineExpt(CoMoment3, "mb-prob", true)
    if (shouldRecord) expt.warmup()
    List(0.1, 0.2, 0.3, 0.4).foreach { prob =>
      println("Microbenchmark for prob = " + prob)
      val cg = MicroBench(10, 100000, 0.5, prob)
      val fullname = cg.inputname + "_all"
      (1 to numIters).foreach { i =>
        println(s"Trial $i/$numIters")
        val (sch, r_its) = cg.generate2()
        sch.initBeforeEncode()
        val dc = new DataCube(MaterializationScheme.all_cuboids(cg.n_bits))
        dc.build(CBackend.b.mkParallel(sch.n_bits, r_its))
        dc.primaryMoments = SolverTools.primaryMoments(dc, false)

        val q = 0 until cg.n_bits
        expt.run(dc, fullname, q, null, sliceValues = Vector())
        dc.cuboids.head.backend.reset
      }
    }
  }

  def moment01[T: ClassTag : Fractional]()(implicit numIters: Int) = {
    println("Running Moment 01")
    import SolverTools.error
    //val solver = new CoMoment4Solver(3, true, Moment0Transformer)
    //val actual = Array(0, 1, 3, 1, 7, 2, 3, 0).map(_.toDouble)
    //solver.add(List(2), Array(5, 12).map(_.toDouble))
    //solver.add(List(0, 1), Array(7, 3, 6, 1).map(_.toDouble))
    //solver.add(List(1, 2), Array(1, 4, 9, 3).map(_.toDouble))
    //solver.add(List(0, 2), Array(3, 2, 10, 2).map(_.toDouble))
    //val mta = solver.momentsToAdd.toMap
    //println("Moments before =" + solver.moments.indices.map(i => mta.getOrElse(i, Double.NegativeInfinity)).mkString(" "))
    //solver.fillMissing()
    //println("Moments after =" + solver.moments.mkString(" "))
    //val result = solver.solve()
    //println(result.mkString(" "))
    //println("Error = " + error(actual, result))

    val cg = SSB(100)
    val param = "15_14_30"
    val sch = cg.schema()

    val isSMS = true
    val ms = (if (isSMS) "sms3" else "rms3")
    val name = s"_${ms}_${param}"
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load2(fullname, cg.inputname + "_base")

    dc.loadPrimaryMoments(cg.inputname + "_base")
    val fileout = new PrintStream(s"expdata/moment01_$ms.csv")
    fileout.println("Query, Moment0Error, Moment1Error")
    val mq = new MaterializedQueryResult(cg)
    List(12).foreach { qs =>
      println(s"Moment01 query $ms size $qs")
      val queries = mq.loadQueries(qs)
      queries.zipWithIndex.foreach { case (qu, qid) => if (qid == 62) {
        println(s"$ms  Query ${qid + 1}/${queries.length}")

        val q = qu.sorted
        val naiveRes = mq.loadQueryResult(qs, qid)

        def solverRes(trans: MomentTransformer[T]) = {
          val l = dc.m.prepare(q, dc.m.n_bits - 1, dc.m.n_bits - 1)
          val fetched = l.map(pm => (pm.accessible_bits, dc.fetch2[T](List(pm)).toArray))
          val primaryMoments = SolverTools.preparePrimaryMomentsForQuery(q, dc.primaryMoments)
          val s = new CoMoment4Solver[T](qu.size, true, trans, primaryMoments)
          fetched.foreach { case (bits, array) => s.add(bits, array) }
          s.fillMissing()
          s.solve()
          (s.moments, s.knownSet, s.solution)
        }

        val (mom0, kS0, solver0Res) = solverRes(Moment0Transformer())
        val (mom1, kS1, solver1Res) = solverRes(Moment1Transformer())

        assert(kS0.sameElements(kS1))
        val total = mom0.head
        val num = implicitly[Fractional[T]]
        val mom0to1 = Moment1Transformer().fromComplementaryMoment(mom0)
        mom0to1.zip(mom1).zipWithIndex.map { case ((m0, m1), i) =>
          val diff = num.abs(num.minus(m0, m1))
          (m0, m1, i, diff)
        }.filter(x => num.gt(num.times(x._4, num.fromInt(1000)), num.zero)).sortBy(x => -num.toDouble(x._4)).take(10).foreach { case (m0, m1, i, diff) =>
          println(s"m[$i] :: $m0 != $m1  diff=${num.toDouble(diff)}")
        }
        solver0Res.zip(solver1Res).zipWithIndex.map { case ((x0, x1), i) =>
          val diff = num.abs(num.minus(x0, x1))
          (x0, x1, i, diff)
        }.filter(x => num.gt(num.times(x._4, num.fromInt(1000)), num.zero)).sortBy(x => -num.toDouble(x._4)).take(10).foreach { case (x0, x1, i, diff) =>
          println(s"x[$i] :: $x0 != $x1  diff=${num.toDouble(diff)}")
        }
        val err0 = error[T](naiveRes, solver0Res)
        val err1 = error(naiveRes, solver1Res)
        println(s"Err0=$err0, Err1=$err1")
        assert(math.abs(err0 - err1) <= math.pow(10, -2))
        //assert(mom0to1 sameElements (mom1))
        fileout.println(s"${qu.size}, ${qu.mkString(";")}, $err0, $err1")
      }
      }
    }
  }

  def solverScaling(batch: Boolean = true)(implicit numIters: Int) = {
    List(15, 16, 17).foreach { nb =>
      println("\n\nMicrobenchmark for Dimensionality = " + nb)
      val cg = MicroBench(nb, 100000, 0.5, 0.25)
      val fullname = cg.inputname + "_all"
      Profiler.resetAll()
      (1 to numIters).foreach { i =>
        //println(s"Trial $i/$numIters")
        val (sch, r_its) = cg.generate2()
        sch.initBeforeEncode()
        val dc = new DataCube(MaterializationScheme.all_cuboids(cg.n_bits))
        dc.build(CBackend.b.mkParallel(sch.n_bits, r_its))
        val moments = SolverTools.primaryMoments(dc, false)
        val q = 0 until cg.n_bits
        val pm2 = SolverTools.preparePrimaryMomentsForQuery[Double](q, moments)
        val s0 = Profiler.profile("s0 Construct") {
          new MomentSolverAll[Double](nb, CoMoment3)
        }
        val s1 = Profiler.profile("s1 Construct") {
          new MomentSolverAll[Double](nb, CoMoment4)
        }
        val s2 = Profiler.profile("s2 Construct") {
          new CoMoment4Solver[Double](nb, true, Moment1Transformer(), pm2)
        }
        val s3 = Profiler.profile("s3 Construct") {
          new CoMoment4Solver[Double](nb, false, Moment1Transformer(), pm2)
        }
        var l = dc.m.prepare(q, nb - 1, nb - 1)
        while (!(l.isEmpty)) {
          val fetched = dc.fetch2[Double](List(l.head))
          val bits = l.head.accessible_bits
          Profiler.profile("s0 Add") {
            s0.add(bits, fetched.toArray)
          }
          Profiler.profile("s1 Add") {
            s1.add(bits, fetched.toArray)
          }
          Profiler.profile("s2 Add") {
            s2.add(bits, fetched.toArray)
          }
          Profiler.profile("s3 Add") {
            s3.add(bits, fetched.toArray)
          }

          if (!batch) {
            Profiler.profile("s0 FillMiss") {
              s0.fillMissing()
            }
            Profiler.profile("s0 Solve") {
              s0.fastSolve()
            }

            Profiler.profile("s1 FillMiss") {
              s1.fillMissing()
            }
            Profiler.profile("s1 Solve") {
              s1.fastSolve()
            }

            Profiler.profile("s2 FillMiss") {
              s2.fillMissing()
            }
            Profiler.profile("s2 Solve") {
              s2.solve()
            }

            Profiler.profile("s3 FillMiss") {
              s3.fillMissing()
            }
            Profiler.profile("s3 Solve") {
              s3.solve()
            }
          }

          l = l.tail
        }

        if (batch) {
          Profiler.profile("s0 FillMiss") {
            s0.fillMissing()
          }
          Profiler.profile("s0 Solve") {
            s0.fastSolve()
          }

          Profiler.profile("s1 FillMiss") {
            s1.fillMissing()
          }
          Profiler.profile("s1 Solve") {
            s1.fastSolve()
          }

          Profiler.profile("s2 FillMiss") {
            s2.fillMissing()
          }
          Profiler.profile("s2 Solve") {
            s2.solve()
          }

          Profiler.profile("s3 FillMiss") {
            s3.fillMissing()
          }
          Profiler.profile("s3 Solve") {
            s3.solve()
          }
        }
        import Tools.round
        //println("Diff = ")
        s1.sumValues.indices.foreach { i =>
          val m0 = round(s0.sumValues(i), 8)
          val m1 = round(s1.sumValues(i), 8)
          val m2 = round(s2.moments(i), 8)
          val m3 = round(s3.moments(i), 8)
          if (m0 != m1 || m0 != m2 || m0 != m3)
            println(s"$i ::: $m0 $m1 $m2 $m3")
        }
        dc.cuboids.head.backend.reset
      }
      Profiler.print()
    }
  }

  def manualSSB(strategy: Strategy, isSMS: Boolean)(implicit shouldRecord: Boolean, numIters: Int): Unit = {
    val cg = SSB(100)
    val sch = cg.schema()
    val encMap = sch.columnVector.map(c => c.name -> c.encoder).toMap[String, ColEncoder[_]]

    val date = encMap("order_date").asInstanceOf[StaticDateCol]
    val year = date.yearCol.bits
    val month = date.monthCol.bits
    val discount = encMap("discount").asInstanceOf[StaticNatCol].bits
    val qty = encMap("quantity").asInstanceOf[StaticNatCol].bits
    val category = encMap("category").asInstanceOf[LazyMemCol].bits
    val brand = encMap("brand").asInstanceOf[LazyMemCol].bits
    val sregion = encMap("supp_region").asInstanceOf[LazyMemCol].bits
    val snation = encMap("supp_nation").asInstanceOf[LazyMemCol].bits
    val scity = encMap("supp_city").asInstanceOf[LazyMemCol].bits
    val cregion = encMap("cust_region").asInstanceOf[LazyMemCol].bits
    val cnation = encMap("cust_nation").asInstanceOf[LazyMemCol].bits
    val ccity = encMap("cust_city").asInstanceOf[LazyMemCol].bits
    val mfgr = encMap("mfgr").asInstanceOf[LazyMemCol].bits

    val queries = collection.mutable.ArrayBuffer[(List[Seq[Int]], String)]()
    queries += List(year, discount, qty) -> "d_year;lo_discount;lo_quantity"
    queries += List(year, brand) -> "d_year;p_brand1"
    queries += List(year, snation, cnation) -> "d_year;s_nation;c_nation"
    queries += List(year.drop(1), ccity.drop(2), scity.drop(2)) -> "d_year/2;c_city/4;s_city/4"
    queries += List(year, snation, category) -> "d_year;s_nation;p_category"

    val param = "15_14_30"
    val ms = (if (isSMS) "sms3" else "rms3")
    val name = s"_${ms}_${param}"
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load2(fullname, cg.inputname + "_base")
    dc.loadPrimaryMoments(cg.inputname + "_base")

    val expname2 = s"manual-ssb-$ms"
    val expt = new NewMomentSolverOnlineExpt(strategy, expname2)
    if (shouldRecord) expt.warmup()


    (1 to numIters).foreach { iter =>
      println(s"Manual SSB Iteration $iter/$numIters")
      queries.zipWithIndex.foreach { case ((cs, qname), i) =>
        val q = cs.reduce(_ ++ _)
        val qsize = q.length
        println(s"  Query $i :: $qname   length = $qsize")
        expt.run(dc, fullname, q, null, true, qname + s" ($qsize-D)", Vector())
      }
    }
  }

  def manualNYC(strategy: Strategy, isSMS: Boolean)(implicit shouldRecord: Boolean, numIters: Int): Unit = {
    val cg = NYC
    val sch = cg.schema()
    val encMap = sch.columnVector.map(c => c.name -> c.encoder).toMap[String, ColEncoder[_]]

    val year = encMap("Issue Date").asInstanceOf[StaticDateCol].yearCol.bits
    val month = encMap("Issue Date").asInstanceOf[StaticDateCol].monthCol.bits
    val state = encMap("Registration State").asInstanceOf[LazyMemCol].bits
    val make = encMap("Vehicle Make").asInstanceOf[LazyMemCol].bits
    val color = encMap("Vehicle Color").asInstanceOf[LazyMemCol].bits
    val ptype = encMap("Plate Type").asInstanceOf[LazyMemCol].bits
    val precinct = encMap("Violation Precinct").asInstanceOf[LazyMemCol].bits
    val lawsect = encMap("Law Section").asInstanceOf[LazyMemCol].bits

    val queries = collection.mutable.ArrayBuffer[(List[Seq[Int]], String)]()
    queries += List(year, month) -> "issue_date_year;issue_date_month"
    queries += List(year.drop(1), state) -> "issue_date_year/2;registration_state"
    queries += List(ptype.drop(2), color.drop(5)) -> "plate_type/4;vehicle_color/32"
    queries += List(make.drop(6), lawsect) -> "vehicle_make/64;law_section"
    queries += List(year.drop(2), precinct.drop(3)) -> "issue_date_year/4;violation_precinct/8"

    val param = "15_14_30"
    val ms = (if (isSMS) "sms3" else "rms3")
    val name = s"_${ms}_${param}"
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load2(fullname, cg.inputname + "_base")
    dc.loadPrimaryMoments(cg.inputname + "_base")

    val expname2 = s"manual-nyc-$ms"
    val expt = new NewMomentSolverOnlineExpt(strategy, expname2)
    if (shouldRecord) expt.warmup()

    (1 to numIters).foreach { iter =>
      println(s"Manual NYC Iteration $iter/$numIters")
      queries.zipWithIndex.foreach { case ((cs, qname), i) =>
        val q = cs.reduce(_ ++ _)
        val qsize = q.length
        println(s"  Query $i :: $qname   length = $qsize ")
        expt.run(dc, fullname, q, null, true, qname + s" ($qsize-D)", Vector())
      }
    }
  }

  def debug(): Unit = {
    implicit val shouldRecord = false
    val cg = SSB(100)
    val isSMS = false
    val param = "15_14"
    val name = (if (isSMS) "_sms_" else "_rms_") + param
    val fullname = cg.inputname + name
    val dc = PartialDataCube.load2(fullname, cg.inputname + "_base")
    dc.loadPrimaryMoments(cg.inputname + "_base")
    val sch = cg.schema()
    //val q1 = Vector(75, 134, 168, 178, 188, 219, 237, 276, 315, 355)
    //val q2 = List(116, 117, 118, 119, 120, 129, 130, 131, 137, 138, 139, 155, 172, 180, 192)
    val q = List(141, 142, 143, 144, 152, 153, 154, 155, 165, 171, 172, 180, 185, 186, 192)
    //val queries = (0 to 4).map(i => Tools.rand_q(429, 10))
    ////val numQs = sch.root.numPrefixUpto(15)
    ////(0 until 15).map(i => println(s"$i => " + numQs(i)))
    //

    //val expt = new UniformSolverFullExpt[Double](fullname)
    //import RationalTools._
    //val expt = new LPSolverFullExpt[Rational](dc, fullname)
    //expt.warmup(10)


    //val sample = Exponential
    //val cg = MBSimple(12)

    //val fullname = cg.inputname + "_all"
    //val dc = DataCube.load2(fullname)

    //val (sch, r_its) = cg.generate2()
    //sch.initBeforeEncode()
    //val dc = new DataCube(MaterializationScheme.all_cuboids(cg.n_bits))
    //dc.build(CBackend.b.mkParallel(sch.n_bits, r_its))

    //val q = 0 until 12
    //val q = sch.root.samplePrefix(15)
    //val res = dc.naive_eval(q)
    //val zeroes = res.filter(_ == 0.0).length
    //val sparse = zeroes.toDouble / res.length
    //val tot = res.sum
    //val naiveM = SolverTools.fastMoments(res).map(x => (x * 10000 / tot).toInt)
    //naiveM.zipWithIndex.sortBy(-_._1).take(15).foreach(println)
    //println("Sparsity = " + sparse)
    //val resMax = res.max.toInt
    //val step = math.max(resMax/1000,1)
    //res.groupBy(x => (x.toInt/step) * step).mapValues(_.length).toList.sortBy(_._1).foreach(println)
    //val fullname = "NYC_sms_16_10"

    //List("NYC_rms_16_10" -> "NYC_base", "SSB-sf100_rms_15_14" -> "SSB-sf100_base").foreach { case (fullname, basename) =>
    //  val dc = PartialDataCube.load2(fullname, basename)
    //  dc.primaryMoments = SolverTools.primaryMoments(dc)
    //  dc.savePrimaryMoments(basename)
    //}
    //List("warmup", "warmupall").foreach{n =>
    //  val dc = DataCube.load2(n)
    //  dc.primaryMoments = SolverTools.primaryMoments(dc)
    //  dc.savePrimaryMoments(n)
    //}
    //val m2 =new EfficientMaterializationScheme(dc.m)
    //val expt = new MomentSolverBatchExpt[Double](fullname)
    val expt = new NewMomentSolverBatchExpt(CoMoment3, fullname)
    (0 until 10).foreach { x => expt.run(dc, fullname, q, null, sliceValues = Vector()) }
    //val expt = new UniformSolverOnlineExpt[Double](fullname, true)
    //queries.foreach { q1 => dc.m.prepare(q1, 50, 400) }
  }

  def main(args: Array[String]) {
    implicit val shouldRecord = true
    implicit val numIters = 100
    import RationalTools._
    val strategy = CoMoment3
    args.lift(0).getOrElse("debug") match {
      case "Fig7" =>
        cuboid_distribution(false)
        cuboid_distribution(true)
      case "Tab1" => storage_overhead()
      case "Fig8" | "lpp" =>
        lpp_query_dimensionality(false)
        lpp_query_dimensionality(true)
      case "Fig9" | "qdims" =>
        moment_query_dimensionality(strategy, false)
        moment_query_dimensionality(strategy, true)
      case "Fig10" | "matparams" =>
        moment_mat_params(strategy, false)
        moment_mat_params(strategy, true)
      case "Fig11" | "microbench" =>
        mb_dims()
        mb_stddev()
        mb_prob()
      case "schema" =>
        schemas()
      case "moment01" => moment01[Rational]()
      case "momentcompare" =>
        momentCompareFixedSlice()
        momentCompareFixedTotal()
      case "Fig12" | "manual" =>
        manualSSB(strategy, true)
        manualNYC(strategy, true)
      case "scaling" => solverScaling(false)
      case _ => trieExpt[Rational]()
    }
  }
}

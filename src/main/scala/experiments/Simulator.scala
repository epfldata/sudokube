package experiments

import core.materialization.{MaterializationSchemeInfo, RandomizedMaterializationScheme}

object Simulator {
  val onemill = 1000 * 1000L
  val ssb1 = (150, 6*onemill)
  val ssb10 = (170, 60*onemill)
  val ssb100 = (190, 600*onemill)
  val base = math.pow(10, 0.19)
  val rfs = List(-30.0, -31.0, -32.0)
  def expt1(): Unit = {


    val rms = rfs.map { lrf =>
      val rf = math.pow(10, lrf)
      val proj = RandomizedMaterializationScheme(ssb100._1, rf, base).projections
      println("Median size = "+ proj(proj.size/2).size)
      lrf -> proj.groupBy(_.length).mapValues(_.length).toList.sortBy(_._1)
    }
    rms.foreach{case (lrf, vs) =>
      println(s"\n\n ${lrf}")
      vs.foreach{ case (d, n) => println(s"$d \t $n")}
    }
  }

  def expt2() = {
    val data = rfs.map { lrf =>
      val rf = math.pow(10, lrf)
      val rms = RandomizedMaterializationScheme(ssb100._1, rf, base)
      val info = new MaterializationSchemeInfo(rms)
      val sizes = List(22, 23, 29, 30).map(s => s -> (info.fd_ratio(s) - 1.0))
      lrf -> sizes
    }

    data.foreach{case (lrf, vs) =>
      println(s"\n\n ${lrf}")
      vs.foreach{ case (s, f) => println(s"$s \t $f")}
    }
  }
  def main(args: Array[String]) {

//expt1()
expt2()

  }


}


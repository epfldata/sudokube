package experiments.plotters

import breeze.io.{CSVReader, CSVWriter}
import breeze.plot._
import org.jfree.chart.axis.{NumberTick, NumberTickUnit}

import java.io.{File, FileReader, PrintStream}


object OnlinePlotter {

  object KEY extends Enumeration {
    val CUBENAME, SOLVERNAME, QUERYID, QSIZE, CUBOIDCOUNTER, TIME, DOF, ERROR, MAXDIM, QUERYSTR, QUERYSTR2, ENTROPY  = Value
  }

  import KEY._

  def getData(name: String, seriesKey: Int, Xkey: Int, Ykey: Int) = {

    val data = CSVReader.read(new FileReader(s"expdata/$name")).tail
    val iterKey = QUERYID.id


    val seriesData = data.
      groupBy(r => r(seriesKey)).
      mapValues {
        _.groupBy(_ (iterKey)).values.map(_.map(r => r(Xkey).toDouble -> r(Ykey).toDouble).toList).toVector
      }
    seriesData
  }

  def getSeries(data: Vector[List[(Double, Double)]], agg: Seq[Double] => Double, initValue: Double, ykey: KEY.Value) = {
    val data2 = data.toArray
    val N = data.size
    //println(s"N = $N")
    val currentValues = Array.fill(N)(initValue)
    val result = collection.mutable.ArrayBuffer[(Double, Double)]()
    var curTime = 0.0
    result += (curTime -> initValue)
    var finished = false
    while (!finished) {
      val (minIdx, minTime, value) = data2.indices.foldLeft((-1, Double.PositiveInfinity, Double.PositiveInfinity)) { case (acc@(_, time, _), idx) =>
        if (data2(idx).isEmpty) acc else if (data2(idx).head._1 < time)
          (idx, data2(idx).head._1, data2(idx).head._2)
        else
          acc
      }
      if (minIdx == -1)
        finished = true
      else {
        if(minTime - curTime >  0.01) {  //we have to sample at this granularity even if the next event takes place much later.
          result += ((minTime-0.011) -> agg(currentValues))
        }
        currentValues(minIdx) = value
        result += (minTime -> agg(currentValues))
        data2(minIdx) = data2(minIdx).tail
        curTime = minTime
      }
    }
    val res1 = result.filter(_._2 < Double.PositiveInfinity).groupBy(x => math.round(x._1 * 100) / 100.0).mapValues(x => x.map(_._2).sum / x.length).toVector.sortBy(_._1)
    val maxt = res1.last._1
    ykey match {
      case ERROR => res1 :+ (maxt + 0.01 -> 0.0)
      case _ => res1
    }
  }


  def myplot(name: String, xkey: KEY.Value, ykey: KEY.Value, seriesKey: KEY.Value) = {

    val isLPP = name.startsWith("LP")

    import KEY._

    val data = getData(name, seriesKey.id, xkey.id, ykey.id)

    def avgf(vs: Seq[Double]) = vs.sum / vs.size

    def minf(vs: Seq[Double]) = vs.min

    def maxf(vs: Seq[Double]) = vs.max

    val initValue = if (isLPP) Double.PositiveInfinity else 1.0

    //val fig = Figure()
    //fig.refresh()
    //val plt = fig.subplot(0)
    //plt.legend = true
    //
    //plt.yaxis.setTickUnit(new NumberTickUnit(if(isDOF) 200 else 0.1))
    //plt.xaxis.setTickUnit(new NumberTickUnit(0.5))


    //plt.xlabel = "Time (s)"

    val t1 = "Mean " + ykey
    //plt.title = t1 + (if(isQuerySize) " for cube log(rf) = -16" else " for query size 10")
    val csvout = new PrintStream(s"expdata/${name.dropRight(4)}-$ykey.csv")

    data.map { case (n, d1) =>
      //val (n,d1) = data.head
      println(s"Averaging over ${d1.length} entries for $n")
      val avg = getSeries(d1, avgf, initValue, ykey)
      //val min = getSeries(d1, minf, initValue)
      //val max = getSeries(d1, maxf, initValue)

      //d2.foreach(println)
      val xavg = avg.map(_._1)
      val yavg = avg.map(_._2)
      csvout.println("Time(s)," + xavg.mkString(","))
      csvout.println(s"$n," + yavg.mkString(","))
      /*
       plt += plot(xavg, yavg, name=n)

       //val xmin = min.map(_._1)
       //val ymin = min.map(_._2)
       //plt += plot(xmin, ymin, name=n+"min" )
       //
       //val xmax = max.map(_._1)
       //val ymax = max.map(_._2)
       //plt += plot(xmax, ymax, name=n+"max" )
  */
    }
    //fig.refresh()
    csvout.close()
    //fig.saveas(s"figs/iowa-online-$filename.pdf")

  }

  def main(args: Array[String]): Unit = {

    //val name = "online_NYC.csv"
    //myplot(name, true, false)
    //myplot(name, false, false)

    //val name2 = "online_SSB-sf100.csv"
    //myplot(name2, false, false)
    //myplot(name2, true, false)

    def argsMap(s: String) = s match {
      case "params" => CUBENAME
      case "qsize" => QSIZE
      case "query" => QUERYSTR2
      case "time" => TIME
      case "dof" => DOF
      case "error" => ERROR
      case "entropy" => ENTROPY
    }

    val name = args(0)
    val seriesKey = args.lift(1).map(argsMap).getOrElse(QSIZE)
    val xkey = args.lift(2).map(argsMap).getOrElse(TIME)
    val ykey = args.lift(3).map(argsMap).getOrElse(ERROR)

    //myplot(name3, DOF, true)
    //myplot(name3, MAXDIM, true)
    myplot(name, xkey, ykey, seriesKey)

  }
}

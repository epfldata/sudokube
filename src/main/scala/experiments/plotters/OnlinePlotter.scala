package experiments.plotters

import breeze.io.{CSVReader, CSVWriter}
import breeze.plot._
import org.jfree.chart.axis.{NumberTick, NumberTickUnit}

import java.io.{File, FileReader, PrintStream}


object OnlinePlotter {

  def getData(name: String, filterf: IndexedSeq[String] => Boolean, groupf: IndexedSeq[String] => String, Ykey: Int) = {

    val data = CSVReader.read(new FileReader(s"expdata/$name")).tail
    val queryKey = 1
    val Xkey = 4

    val seriesData = data.
      filter(filterf).
      groupBy(groupf).
      mapValues {
        _.groupBy(_ (queryKey)).values.map(_.map(r => r(Xkey).toDouble -> r(Ykey).toDouble).toList).toVector
      }
  seriesData
  }

  def getSeries(data: Vector[List[(Double, Double)]], agg: Seq[Double] => Double, initValue: Double) = {
    val data2 = data.toArray
    val N = data.size
    println(s"N = $N")
    val currentValues = Array.fill(N)(initValue)
    val result = collection.mutable.ArrayBuffer[(Double, Double)]()

    result += ((0.0) -> initValue)
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
        currentValues(minIdx) = value
        result += (minTime -> agg(currentValues))
        data2(minIdx) = data2(minIdx).tail
      }
    }
    result.groupBy(x => math.round(x._1*100)/100.0).mapValues(x => x.map(_._2).sum/x.length).toVector.sortBy(_._1)
  }

  def myplot(name: String, isDOF: Boolean, isQuerySize: Boolean) = {
    def filterCube(r: IndexedSeq[String]) = r(0).contains("15_25_3")
    def filterQuery(r: IndexedSeq[String]) = r(2) == "15"

    def groupCube(r: IndexedSeq[String]) = r(0)
    def groupQuery(r: IndexedSeq[String]) = r(2)

    def filterf(r: IndexedSeq[String]) = if(isQuerySize) filterCube(r) else filterQuery(r)
    def groupf(r:IndexedSeq[String]) = if(isQuerySize) groupQuery(r) else groupCube(r)

    val valueKey = if(isDOF) 5 else 6
    val data = getData(name, filterf, groupf, valueKey)

    def avgf(vs :Seq[Double]) = vs.sum/vs.size
    def minf(vs: Seq[Double]) = vs.min
    def maxf(vs: Seq[Double]) = vs.max

    val initValue = 1.0

    //val fig = Figure()
    //fig.refresh()
    //val plt = fig.subplot(0)
    //plt.legend = true
    //
    //plt.yaxis.setTickUnit(new NumberTickUnit(if(isDOF) 200 else 0.1))
    //plt.xaxis.setTickUnit(new NumberTickUnit(0.5))
    val filename = (if(isDOF)"dof-" else "error-") + (if(isQuerySize) "qs" else "rf")

    //plt.xlabel = "Time (s)"

    val t1 = (if(isDOF) "Mean DOF" else "Mean Error")
    //plt.title = t1 + (if(isQuerySize) " for cube log(rf) = -16" else " for query size 10")
    val csvout = new PrintStream(s"expdata/$name-$filename.csv")

    data.map { case (n, d1) =>
      //val (n,d1) = data.head
      val avg = getSeries(d1, avgf, initValue)
      //val min = getSeries(d1, minf, initValue)
      //val max = getSeries(d1, maxf, initValue)

      //d2.foreach(println)
      val xavg = avg.map(_._1)
      val yavg = avg.map(_._2)
      csvout.println("Time(s),"+xavg.mkString(","))
      csvout.println(s"$n," +yavg.mkString(","))
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

    val name3 = "online_SSB-sf100-qs.csv"
    myplot(name3, false, true)
    myplot(name3, true, true)

  }
}

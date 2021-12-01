package experiments.plotters

import breeze.io.CSVReader
import breeze.plot._
import org.jfree.chart.axis.NumberTickUnit

import java.io.FileReader


object OnlinePlotter {

  def getData(name: String) = {

    val data = CSVReader.read(new FileReader(s"expdata/$name")).tail
    val seriesKey = 0
    val filterKey = 2
    val queryKey = 1
    val Xkey = 4
    val Ykey = 6
    val seriesData = data.
      //filter(r => r(filterKey).contains("--16.0-")).
      filter(r => r(filterKey) == "12").
      groupBy(_ (seriesKey)).
      mapValues {
        _.groupBy(_ (queryKey)).values.map(_.map(r => r(Xkey).toDouble -> r(Ykey).toDouble).toList).toVector
      }
  seriesData
  }

  def average(data: Vector[List[(Double, Double)]]) = {
    val data2 = data.toArray
    val N = data.size
    println(s"N = $N")
    val currentError = Array.fill(N)(1.0)
    val result = collection.mutable.ArrayBuffer[(Double, Double)]()

    result += ((0.0) -> 1.0)
    var finished = false
    while (!finished) {
      val (minIdx, minTime, error) = data2.indices.foldLeft((-1, Double.PositiveInfinity, Double.PositiveInfinity)) { case (acc@(_, time, _), idx) =>
        if (data2(idx).isEmpty) acc else if (data2(idx).head._1 < time)
          (idx, data2(idx).head._1, data2(idx).head._2)
        else
          acc
      }
      if (minIdx == -1)
        finished = true
      else {
        currentError(minIdx) = error
        result += (minTime -> currentError.sum/N)
        data2(minIdx) = data2(minIdx).tail
      }
    }
    result.groupBy(x => math.round(x._1*100)/100.0).mapValues(x => x.map(_._2).sum/x.length).toVector.sortBy(_._1)
  }


  def main(args: Array[String]): Unit = {
    val data = getData("online_IowaAll--16-0.19_dummy.csv")
    //println(data.length)
    val fig = Figure()
    val plt = fig.subplot(0)
    plt.legend = true
    plt.yaxis.setTickUnit(new NumberTickUnit(0.1))
    data.map { case (n, d1) =>
    //val (n,d1) = data.head
      val d2 = average(d1)

      //d2.foreach(println)
      val x = d2.map(_._1)
      val y = d2.map(_._2)
      plt += plot(x, y, name=n)

    }

    //Thread.sleep(10 * 100000L)
    fig.saveas("figs/test2.pdf")
  }
}

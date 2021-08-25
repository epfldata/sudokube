package frontend

import core._
import schema._

import scala.swing._
import org.jfree.chart._
import org.jfree.data.xy._

import java.awt.{BasicStroke, Color}
import javax.swing.JPanel

case class FeatureDisplay(schema: Schema, dc: DataCube, draw: (Schema, Seq[(Interval[Rational], Int)]) => String) {


  import org.jfree.chart.ChartFactory
  import org.jfree.chart.JFreeChart
  import org.jfree.chart.axis.NumberAxis
  import org.jfree.chart.plot.PlotOrientation
  import org.jfree.chart.plot.XYPlot
  import org.jfree.chart.renderer.xy.DeviationRenderer
  import org.jfree.ui.RectangleInsets
  import org.jfree.data.xy.YIntervalSeries
  import org.jfree.data.xy.YIntervalSeriesCollection

  val dataset = new YIntervalSeriesCollection
  //val ser = new YIntervalSeries("Series initial")
  //(0 to 100).map(i => ser.add(i, 2 * i, i, 3 * i))
  //dataset.removeAllSeries()
  //dataset.addSeries(ser)
  /*
  private def createDataset: XYDataset = {
    val series1 = new YIntervalSeries("Series 1")
    val series2 = new YIntervalSeries("Series 2")
    var y1 = 100.0
    var y2 = 100.0
    for (i <- 0 to 100) {
      y1 = y1 + Math.random - 0.48
      val dev1 = 0.05 * i
      series1.add(i, y1, y1 - dev1, y1 + dev1)
      y2 = y2 + Math.random - 0.50
      val dev2 = 0.07 * i
      series2.add(i, y2, y2 - dev2, y2 + dev2)
    }
    series1

    dataset.addSeries(series1)
    //dataset.addSeries(series2)
    dataset
  }
*/

  val chart = createChart(dataset)
  val ui = new MainFrame {
    title = "sudokube"
    preferredSize = new Dimension(600, 400)

    val jp = new BorderPanel {
      peer.add(new ChartPanel(chart))
    }
    contents = jp
  }

  ui.visible = true

  private def createChart(dataset: XYDataset) = { // create the chart...
    val chart = ChartFactory.createXYLineChart("DeviationRenderer - Demo 1", // chart title
      "X", // x axis label
      "Y", // y axis label
      dataset, // data
      PlotOrientation.VERTICAL, true, // include legend
      true, // tooltips
      false) // urls)
    chart.setBackgroundPaint(Color.white)
    // get a reference to the plot for further customisation...
    val plot = chart.getPlot.asInstanceOf[XYPlot]
    plot.setBackgroundPaint(Color.lightGray)
    plot.setAxisOffset(new RectangleInsets(5.0, 5.0, 5.0, 5.0))
    plot.setDomainGridlinePaint(Color.white)
    plot.setRangeGridlinePaint(Color.white)
    val renderer = new DeviationRenderer(true, false)
    renderer.setSeriesStroke(0, new BasicStroke(3.0f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND))
    renderer.setSeriesStroke(0, new BasicStroke(3.0f))
    //renderer.setSeriesStroke(1, new BasicStroke(3.0f))
    renderer.setSeriesFillPaint(0, new Color(200, 200, 255))
    //renderer.setSeriesFillPaint(1, new Color(255, 200, 200))
    plot.setRenderer(renderer)
    // change the auto tick unit selection to integer units only...
    val yAxis = plot.getRangeAxis.asInstanceOf[NumberAxis]
    yAxis.setAutoRangeIncludesZero(false)
    yAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits)
    chart
  }

  def run(xcols: List[Int], zcols: List[Int], zval: Int)(cheap_size: Int) = {

    import RationalTools._

    val q = xcols ++ zcols
    var count = 0

    def callback(s: SparseSolver[Rational]) = {
      s.propagate_bounds(0 to s.n_vars - 1)


      val series = new YIntervalSeries("Series 1")
      val selectedbounds = s.bounds.zipWithIndex.filter {
        case (r, id) => (id / (1 << xcols.length)) == zval
      }
      selectedbounds.foreach { case (r, id) =>
        val x = id % (1 << xcols.length)
        val ylow = r.lb.getOrElse(Rational(0, 1)).toDouble
        val yhigh = r.ub.getOrElse(Rational(100, 1)).toDouble
        val ymid = (ylow + yhigh) / 2.0
        series.add(x, ymid, ylow, yhigh)
      }

      dataset.removeAllSeries()
      dataset.addSeries(series)
      println(draw(schema, selectedbounds))
      count = count + 1
      chart.fireChartChanged()
      Thread.sleep(1000)
      true
    }

    dc.online_agg[Rational](q, cheap_size, callback)
  }
}

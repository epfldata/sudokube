package frontend.gui


import core._
import frontend.schema._
import org.jfree.chart.{ChartFactory, ChartPanel}
import org.jfree.chart.axis.NumberAxis
import org.jfree.chart.plot.{PlotOrientation, XYPlot}
import org.jfree.chart.renderer.xy.DeviationRenderer
import org.jfree.data.xy.{YIntervalSeries, YIntervalSeriesCollection}
import org.jfree.ui.RectangleInsets
import util.BigBinary

import java.awt.{BasicStroke, BorderLayout, Color}
import javax.swing.JButton
import scala.swing.BorderPanel.Position.{Center, North, South}
import scala.swing.ComboBox.stringEditor
import scala.swing._
import scala.swing.event.{ButtonClicked, SelectionChanged}

case class FeatureFrame(sch: Schema, dc: DataCube, cheap_size: Int) {
  val dataset = new YIntervalSeriesCollection

  val chart = {
    val chart = ChartFactory.createXYLineChart("", sch.columnList.head._1, "Sales", dataset,
      PlotOrientation.VERTICAL,
      false, false, false
    )
    chart.setBackgroundPaint(Color.white)
    val plot = chart.getPlot.asInstanceOf[XYPlot]
    plot.setBackgroundPaint(Color.white)
    plot.setAxisOffset(new RectangleInsets(5.0, 5.0, 5.0, 5.0))
    plot.setDomainGridlinePaint(Color.white)
    plot.setRangeGridlinePaint(Color.white)
    val renderer = new DeviationRenderer(true, false)
    renderer.setSeriesStroke(0, new BasicStroke(3.0f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND))
    renderer.setSeriesFillPaint(0, new Color(255, 200, 200))
    plot.setRenderer(renderer)
    // change the auto tick unit selection to integer units only...
    val yAxis = plot.getRangeAxis.asInstanceOf[NumberAxis]
    yAxis.setAutoRangeIncludesZero(true)
    yAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits)

    val xAxis = plot.getDomainAxis.asInstanceOf[NumberAxis]
    xAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits)
    chart
  }

  case class DimensionPanel(dimname: String, low: Int, high: Int, isColX: Boolean = false) extends GridBagPanel {
    var cur = if (isColX) high-1 else high + 1

    def colsList = (cur to high).toList

    val plusButton = new Button("+")
    val minusButton = new Button("-")
    val dimlabel = new Label(dimname)

    def colsToText = cur + ":" + high

    val colsLabel = new Label(colsToText)

    def filtvalue = {
      val sel = filterbox.selection.item
      if (sel == "<all>")
        List()
      else sel.toCharArray.map(_ - '0').toList
    }

    def filtList = {
      if (!isColX) {
        val res = (0 until 1 << (high + 1 - cur)).map(v => BigBinary(v).toPaddedString(high + 1 - cur))
        if (res.size <= 1)
          List("<all>")
        else res
      } else Nil
    }

    val filterbox = new ComboBox[String](filtList)

    listenTo(plusButton, minusButton, filterbox.selection)
    reactions += {
      case ButtonClicked(`plusButton`) =>
        if (cur <= high) {
          cur = cur + 1
          colsLabel.text = colsToText
          filterbox.peer.setModel(ComboBox.newConstantModel(filtList))
          restart()
        }
      case ButtonClicked(`minusButton`) =>
        if (cur > low) {
          cur = cur - 1
          colsLabel.text = colsToText
          filterbox.peer.setModel(ComboBox.newConstantModel(filtList))
          restart()
        }
      case SelectionChanged(`filterbox`) =>
        restart()
    }
    val dummyLabel = new Label("")
    layout(dimlabel) = (0, 0)
    layout(minusButton) = (1, 0)
    layout(colsLabel) = (2, 0)
    layout(plusButton) = (3, 0)
    filterbox.prototypeDisplayValue = Some(" " * Math.max(10, high + 5 - low))
    //filterbox.peer.setSize(100, filterbox.preferredSize.height)
    //dummyLabel.peer.setSize(100, filterbox.preferredSize.height)
    //filterbox.preferredSize = new Dimension(100, -1)
    //dummyLabel.preferredSize = new Dimension(100, -1)
    if (isColX)
      layout(dummyLabel) = (4, 0)
    else
      layout(filterbox) = (4, 0)
  }

  val chartPanel = new BorderPanel {
    peer.add(new ChartPanel(chart))
  }

  val dimMap = sch.columnList.zipWithIndex.map{case ((n, ce), id) => {
    id -> DimensionPanel(n, ce.bits.min, ce.bits.max, id == 0)
  }}.toMap

  val innerCP = new GridBagPanel {
    dimMap.foreach { case (id, d) => layout(d) = (0, id) }
  }

  val controlPanel = new ScrollPane {
    contents = innerCP
  }
  val mainPanel = new BorderPanel {
    layout(chartPanel) = Center
    layout(controlPanel) = South
  }
  val ui = new MainFrame {
    title = "sudokube"
    preferredSize = new Dimension(600, 550)
    contents = mainPanel
  }
  ui.visible = true
  var task = new BackgroundTask
  var thread = new Thread(task)
  thread.start()
  def restart(): Unit = {
    task.stop = true
    thread.stop()
    dataset.removeAllSeries()
    chart.fireChartChanged()
    //while (!task.finish)
      Thread.sleep(200)
    task = new BackgroundTask
    thread = new Thread(task)
      thread.start()
  }

  class BackgroundTask extends Runnable {
    @volatile var stop = false
    @volatile var finish = false

    override def run() {
      val aggdim = dimMap(0)
      val filtdims = dimMap.filterKeys(_ > 0).toList.sortBy(_._1)
      val aggcols = aggdim.colsList
      val filtcols = filtdims.map{ case (id, d) => d.colsList }.foldLeft[List[Int]](Nil)(_ ++ _)
      val filtvalueList = filtdims.map { case (id, d) => d.filtvalue }.foldLeft[List[Int]](Nil)(_ ++ _)
      val filtvalue = filtvalueList.foldLeft((0, 0)) { case ((sum, power), cur) => (sum + (cur << power), power + 1) }._1
      val query = aggcols ++ filtcols
      println("\nQUERY = " + query.mkString(","))
      println("AGGCOLS = " + aggcols.mkString(","))
      println("FILTERCOLS = " + filtcols.mkString(","))
      println("FILTVALUELIST = " + filtvalueList.mkString(","))
      println("FILTVALUE = " + filtvalue)

      def cond(id: Int) = (id / (1 << aggcols.length)) == filtvalue

      def callback(s: SparseSolver[Rational]) = {
        s.propagate_bounds(0 to s.n_vars - 1)
        val series = new YIntervalSeries("")
        val allbounds = s.bounds.zipWithIndex
        //allbounds.foreach { case (r, id) =>
        //  val selected = cond(id)
        //  println(selected + "   " + id + " :: " + BigBinary(id).toPaddedString(32) + " ==> " + r + " :: " + BigBinary(r.lb.get.toInt).toPaddedString(32))
        //}
        val selectedbounds = allbounds.filter {
          case (r, id) => cond(id)
        }

        selectedbounds.foreach { case (r, id) =>
          val selected = cond(id)
          println(selected + "   " + id + " :: " + id.toBinaryString + " ==> " + r + " :: " +  r.ub.get.toInt.toBinaryString)
        }
        selectedbounds.foreach { case (r, id) =>
          val x = id % (1 << aggcols.length)
          val ylow = r.lb.get.toDouble
          val yhigh = r.ub.get.toDouble
          val ymid = (ylow + yhigh) / 2.0
          series.add(x, ymid, ylow, yhigh)
        }

        dataset.removeAllSeries()
        dataset.addSeries(series)
        chart.fireChartChanged()
        //Thread.sleep(1000)
        !stop
      }
      import RationalTools._
      dc.online_agg(query, cheap_size, callback, cond)
      finish = true
    }
  }

}

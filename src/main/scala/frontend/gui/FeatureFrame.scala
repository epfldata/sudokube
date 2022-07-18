package frontend.gui


import core._
import core.solver.lpp.SparseSolver
import core.solver.{Rational, RationalTools}
import frontend.schema._
import org.jfree.chart.axis.NumberAxis
import org.jfree.chart.plot.{PlotOrientation, XYPlot}
import org.jfree.chart.renderer.xy.DeviationRenderer
import org.jfree.chart.{ChartFactory, ChartPanel}
import org.jfree.data.xy.{YIntervalSeries, YIntervalSeriesCollection}
import org.jfree.ui.RectangleInsets
import util.BigBinary

import java.awt.{BasicStroke, Color}
import scala.swing.BorderPanel.Position.{Center, South}
import scala.swing._
import scala.swing.event.{ButtonClicked, SelectionChanged}

case class FeatureFrame(sch: Schema2, dc: DataCube, cheap_size: Int) {
  val dataset = new YIntervalSeriesCollection
  var naiveSeries: YIntervalSeries =  null
  var lppSeries: YIntervalSeries = null

  val chart = {
    val chart = ChartFactory.createXYLineChart("", sch.columnVector.head.name, "Sales", dataset,
      PlotOrientation.VERTICAL,
      true, false, false
    )
    chart.setBackgroundPaint(Color.white)
    val plot = chart.getPlot.asInstanceOf[XYPlot]
    plot.setBackgroundPaint(Color.white)
    plot.setAxisOffset(new RectangleInsets(5.0, 5.0, 5.0, 5.0))
    plot.setDomainGridlinePaint(Color.white)
    plot.setRangeGridlinePaint(Color.white)
    val renderer = new DeviationRenderer(true, false)
    renderer.setSeriesStroke(0, new BasicStroke(1.0f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND, 1.0f, Array[Float](2.0f, 6.0f),
    0.0f))
    renderer.setSeriesStroke(1, new BasicStroke(3.0f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND))
    renderer.setSeriesPaint(1, new Color(255, 0, 0))
    renderer.setSeriesFillPaint(1, new Color(255, 200, 200))
    renderer.setSeriesPaint(0, new Color(128, 128, 128))
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

    def colsList = (cur to high)

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

  val dimMap = sch.columnVector.zipWithIndex.map{case (LD2(n, ce), id) => {
    id -> DimensionPanel(n, ce.bits.min, ce.bits.max, id == 0)
  }}.toMap

  val innerCP = new GridBagPanel {
    dimMap.foreach { case (id, d) => layout(d) = (0, id) }
    val naiveBut = new Button("Naive")
    val solverBut = new Button("Solver")
    layout(naiveBut) = (1, 0)
    layout(solverBut) = (1, 1)
    listenTo(naiveBut, solverBut)
    reactions += {
      case ButtonClicked(`naiveBut`) =>
        if(task != null) task.stop = true
        if(thread != null) thread.stop()
        Thread.sleep(200)
        task = new BackgroundTask(0)
        thread = new Thread(task)
        thread.start()
      case ButtonClicked(`solverBut`) =>
        if(task != null) task.stop = true
        if(thread != null) thread.stop()
        Thread.sleep(200)
        task = new BackgroundTask(1)
        thread = new Thread(task)
        thread.start()
    }
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
  var task: BackgroundTask = null
  var thread : Thread = null

  def restart(): Unit = {
    if (task != null)
      task.stop = true
    if(thread != null) thread.stop()
    dataset.removeAllSeries()
    chart.fireChartChanged()
    //while (!task.finish)
      Thread.sleep(200)
   task = null
  }

  case class BackgroundTask(taskid: Int) extends Runnable {
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

      def callbackNaive(result: Array[Double]) = {
        val series = new YIntervalSeries("Naive")
        val selectedRes = result.zipWithIndex.filter {
          case (r, id) => cond(id)
        }

        selectedRes.foreach {case (y, id) =>
          val x = id % (1 << aggcols.length)
          val y2 = y
          println(s"x=$x y=$y2")
          series.add(x, y2, y2, y2)
        }
        if(naiveSeries != null) dataset.removeSeries(naiveSeries)
        naiveSeries = series
        dataset.addSeries(naiveSeries)
      }

      def callbackLPP(s: SparseSolver[Rational]) = {
        s.propagate_bounds(0 to s.n_vars - 1)
        val series = new YIntervalSeries("LPP")
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

        if(lppSeries != null) dataset.removeSeries(lppSeries)
        lppSeries = series
        dataset.addSeries(lppSeries)
        chart.fireChartChanged()
        //Thread.sleep(1000)
        !stop
      }
      import RationalTools._
      taskid match {
        case 0 => callbackNaive(dc.naive_eval(query))
        case 1 => dc.online_agg(query, cheap_size, callbackLPP, cond)
      }
      finish = true
    }
  }

}

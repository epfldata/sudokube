package frontend.gui

import core._
import core.solver.{Rational, RationalTools}
import core.solver.lpp.SparseSolver
import core.solver.moment.MomentSolverAll
import frontend.generators.SSB
import frontend.schema.encoders.{LazyMemCol, StaticDateCol}
import org.jfree.chart.axis.NumberAxis
import org.jfree.chart.plot.{PlotOrientation, XYPlot}
import org.jfree.chart.renderer.xy.DeviationRenderer
import org.jfree.chart.{ChartFactory, ChartPanel}
import org.jfree.data.xy.{YIntervalSeries, YIntervalSeriesCollection}
import org.jfree.ui.RectangleInsets

import java.awt.{BasicStroke, Color}
import scala.swing.BorderPanel.Position.{Center, South}
import scala.swing._
import scala.swing.event.{ButtonClicked, SelectionChanged}

case class FeatureFrameSSB(sf: Int, dc: DataCube, cheap_size: Int) {
  val dataset = new YIntervalSeriesCollection

  var naiveSeries =  new YIntervalSeries("Naive")
  var lppSeries = new YIntervalSeries("LPP")
  var momentSeries = new YIntervalSeries("Moment")

  dataset.addSeries(naiveSeries)
  dataset.addSeries(lppSeries)
  dataset.addSeries(momentSeries)

  val sch = SSB(sf).schema()
  sch.initBeforeDecode()
  val cols = sch.columnVector

  val odateCol = cols(0).encoder.asInstanceOf[StaticDateCol]
  val oyearB = odateCol.yearCol.bits
  val oyearQtrB = odateCol.quarterCol.bits ++ odateCol.yearCol.bits
  val oyearMonthB = odateCol.monthCol.bits ++ odateCol.yearCol.bits
  val oymdB = odateCol.dayCol.bits ++ odateCol.monthCol.bits ++ odateCol.yearCol.bits

  val timeDim = DimensionPanel("Time", Vector(oyearB, oyearQtrB, oyearMonthB, oymdB), Vector("Year", "YQ", "YM", "YMD"), Vector(), true)
  def getBV(i: Int) = {
    val e = cols(i).encoder
  val lmc = e.asInstanceOf[LazyMemCol]
    (lmc.bits, lmc.decode_map)

  }
  val (nilB, nilV) = (List[Int](), Vector[String]())

  val (ccityB, ccityV) = getBV(11)
  val (cnatB, cnatV) = getBV(12)
  val (cregB, cregV) = getBV(13)

  val custDim = DimensionPanel("Customer", Vector(nilB, cregB, cnatB, ccityB), Vector("", "Region", "Nation", "City"), Vector(nilV, cregV, cnatV, ccityV))

  val (mfgrB, mfgrV) = getBV(18)
  val (categoryB, categoryV) = getBV(19)
  val (colorB, colorV) = getBV(21)

  val partDim = DimensionPanel("Part", Vector(nilB, mfgrB, categoryB, colorB), Vector("", "Mfgr", "Category", "Color"), Vector(nilV, mfgrV, categoryV, colorV))
  val chart = {
    val chart = ChartFactory.createXYLineChart("", "Time", "Sales", dataset,
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
    renderer.setSeriesStroke(0, new BasicStroke(3.0f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND, 1.0f, Array[Float](2.0f, 6.0f),
    0.0f))
    renderer.setSeriesStroke(1, new BasicStroke(1.0f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND))
    renderer.setSeriesStroke(2, new BasicStroke(2.0f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND))
    renderer.setSeriesPaint(1, new Color(255, 0, 0))
    renderer.setSeriesPaint(2, new Color(0, 0, 255))
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

  case class DimensionPanel(dimname: String, queryLevels: Vector[Seq[Int]], levelNames: Vector[String], decodeMap: Vector[IndexedSeq[String]], isColX: Boolean = false) extends GridBagPanel {
    var cur = 0

    def colsList = queryLevels(cur).toList

    val plusButton = new Button("+")
    val minusButton = new Button("-")
    val dimlabel = new Label(dimname)

    def colsToText = levelNames(cur)

    val colsLabel = new Label(colsToText)

    def filtvalue = {
      val sel = filterbox.selection.index
      val total = 1 << queryLevels(cur).size
      (sel,total)
    }

    def filtList = {
      if (!isColX) {
        val res = decodeMap(cur)
        if (res.size <= 1)
          List("<all>")
        else res
      } else Nil
    }

    val filterbox = new ComboBox[String](filtList)

    listenTo(plusButton, minusButton, filterbox.selection)
    reactions += {
      case ButtonClicked(`plusButton`) =>
        if (cur < queryLevels.length-1) {
          cur = cur + 1
          colsLabel.text = colsToText
          filterbox.peer.setModel(ComboBox.newConstantModel(filtList))
          restart()
        }
      case ButtonClicked(`minusButton`) =>
        if (cur > 0) {
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
    filterbox.prototypeDisplayValue = Some(" " * 10)
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


  val dimMap = Map(0 -> timeDim, 1 -> custDim, 2->partDim)
  val measureScale = math.pow(10, -6) * sf
  val innerCP = new GridBagPanel {
    dimMap.foreach { case (id, d) => layout(d) = (0, id) }
    val naiveBut = new Button("Naive")
    val lppBut = new Button("LPP Solver")
    val miBut = new Button("Moment Solver")
    layout(naiveBut) = (1, 0)
    layout(lppBut) = (1, 1)
    layout(miBut) = (1, 2)
    listenTo(naiveBut, lppBut, miBut)
    reactions += {
      case ButtonClicked(but) =>
        val taskid = but match {
          case `naiveBut` => 0
          case `lppBut` => 1
          case `miBut` => 2
        }
        if(task != null) task.stop = true
        if(thread != null) thread.stop()
        Thread.sleep(200)
        task = new BackgroundTask(taskid)
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
    //dataset.removeAllSeries()
    naiveSeries.clear()
    lppSeries.clear()
    momentSeries.clear()
    //dataset.addSeries(naiveSeries)
    //dataset.addSeries(lppSeries)
    //dataset.addSeries(momentSeries)
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
      val filtvalue = filtdims.map { case (id, d) => d.filtvalue}.foldLeft((0,1)){ case (acc@(accsum, acctotal), cur@(cursum, curtotal)) =>
        println(s"acc = $acc cur =$cur")
        (accsum + cursum* acctotal , acctotal * curtotal) }._1
      val query = aggcols ++ filtcols
      println("\nQUERY = " + query.mkString(","))
      println("AGGCOLS = " + aggcols.mkString(","))
      println("FILTERCOLS = " + filtcols.mkString(","))
      println("FILTVALUE = " + filtvalue)

      def cond(id: Int) = (id / (1 << aggcols.length)) == filtvalue

      def callBackMI(s: MomentSolverAll[Double]) = {
        val result = s.solution
        //val series = new YIntervalSeries("Moment")
        momentSeries.clear()
        val selres = result.zipWithIndex.filter{case (r, id) => cond(id)}
        selres.foreach{case (y, id) =>
          val x = id % (1 << aggcols.length)
          val y2 = y * measureScale
          //println(s"x=$x y=$y2")
          momentSeries.add(x, y2, y2, y2)
        }
        //momentSeries = series
        //dataset.removeAllSeries()
        //dataset.addSeries(naiveSeries)
        //dataset.addSeries(lppSeries)
        //dataset.addSeries(momentSeries)
        true
      }
      def callbackNaive(result: Array[Double]) = {
        //val series = new YIntervalSeries("Naive")
        naiveSeries.clear()
        val selectedRes = result.zipWithIndex.filter {
          case (r, id) =>
            val sel =  cond(id)
            //println(s"id = $id  selected = $sel ${id/(1<<aggcols.length)} $filtvalue value=${r*measureScale}")
           sel
        }

        selectedRes.foreach {case (y, id) =>
          val x = id % (1 << aggcols.length)
          val y2 = y * measureScale
          //println(s"x=$x y=$y2")
          naiveSeries.add(x, y2, y2, y2)
        }
        //naiveSeries = series
        //dataset.removeAllSeries()
        //dataset.addSeries(naiveSeries)
        //dataset.addSeries(lppSeries)
        //dataset.addSeries(momentSeries)
      }

      def callbackLPP(s: SparseSolver[Rational]) = {
        s.propagate_bounds(0 to s.n_vars - 1)
        //val series = new YIntervalSeries("LPP")
        lppSeries.clear()
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
          val ylow = r.lb.get.toDouble * measureScale
          val yhigh = r.ub.get.toDouble * measureScale
          val ymid = (ylow + yhigh) / 2.0
          lppSeries.add(x, ymid, ylow, yhigh)
        }
        //lppSeries = series
        //dataset.removeAllSeries()
        //dataset.addSeries(naiveSeries)
        //dataset.addSeries(lppSeries)
        //dataset.addSeries(momentSeries)
        !stop
      }
      import RationalTools._
      taskid match {
        case 0 => callbackNaive(dc.naive_eval(query))
        case 1 => dc.online_agg(query, cheap_size, callbackLPP, cond)
        case 2 => dc.online_agg_moment(query, 1, callBackMI)
      }
      finish = true
    }
  }

}

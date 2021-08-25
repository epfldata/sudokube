package frontend.gui

import core.{Rational, SparseSolver}
import org.jfree.chart.{ChartFactory, ChartPanel}
import org.jfree.chart.axis.NumberAxis
import org.jfree.chart.plot.{PlotOrientation, XYPlot}
import org.jfree.chart.renderer.xy.DeviationRenderer
import org.jfree.data.xy.{YIntervalSeries, YIntervalSeriesCollection}
import org.jfree.ui.RectangleInsets

import java.awt.{BasicStroke, BorderLayout, Color}
import javax.swing.JButton
import scala.swing.BorderPanel.Position.{Center, North, South}
import scala.swing.ComboBox.stringEditor
import scala.swing._
import scala.swing.event.ButtonClicked

case class FeatureFrame() {
  val dataset = new YIntervalSeriesCollection

  val chart = {
    val chart = ChartFactory.createXYLineChart("", "Time", "Sales", dataset,
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
    yAxis.setAutoRangeIncludesZero(false)
    yAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits)
    chart
  }


  var xcolslow = 5
  var ycolslow = 9
  var zcolslow = 12
  var xcolshigh = 8
  var ycolshigh = 11
  var zcolshigh = 10

  def xcolslist = (xcolslow to xcolshigh).toList
  def ycolslist = (ycolslow to ycolshigh).toList
  def zcolslist = (zcolslow to zcolshigh).toList
  val xplus = new Button("+")
  val xminus = new Button("-")
  val yplus = new Button("+")
  val yminus = new Button("-")
  val zplus = new Button("+")
  val zminus = new Button("-")
  val xlabel = new Label("X")
  val ylabel = new Label("Y")
  val zlabel = new Label("Z")

  def xcolslabeltext = xcolslow + ":" + xcolshigh

  def ycolslabeltext = ycolslow + ":" + ycolshigh

  def zcolslabeltext = zcolslow + ":" + zcolshigh

  val xcolslabel = new Label(xcolslabeltext)
  val ycolslabel = new Label(ycolslabeltext)
  val zcolslabel = new Label(zcolslabeltext)

  def yfiltlist = {
    val res = (0 until 1 << (ycolshigh + 1 - ycolslow)).map(_.toBinaryString)
    if (res.size <= 1)
      List("<all>")
    else res
  }

  def zfiltlist = {
    val res = (0 until 1 << (zcolshigh + 1 - zcolslow)).map(_.toBinaryString)
    if (res.size <= 1)
      List("<all>")
    else res
  }


  val yfilt = new ComboBox[String](yfiltlist)
  val zfilt = new ComboBox[String](zfiltlist)

  def yval = yfilt.selection.item
  def zval = zfilt.selection.item
  val chartPanel = new BorderPanel {
    peer.add(new ChartPanel(chart))
  }
  val innerCP = new GridBagPanel {
    layout(xlabel) = (0, 0)
    layout(ylabel) = (0, 1)
    layout(zlabel) = (0, 2)

    layout(xminus) = (1, 0)
    layout(yminus) = (1, 1)
    layout(zminus) = (1, 2)

    layout(xcolslabel) = (2, 0)
    layout(ycolslabel) = (2, 1)
    layout(zcolslabel) = (2, 2)

    layout(xplus) = (3, 0)
    layout(yplus) = (3, 1)
    layout(zplus) = (3, 2)

    layout(yfilt) = (4, 1)
    layout(zfilt) = (4, 2)

  }

  val controlPanel = new BorderPanel {
    layout(innerCP) = Center
  }
  val mainPanel = new BorderPanel {
    layout(chartPanel) = North
    layout(controlPanel) = South
  }
  val ui = new MainFrame {
    title = "sudokube"
    preferredSize = new Dimension(600, 400)
    contents = mainPanel
    listenTo(xplus, xminus, yplus, yminus, zplus, zminus, yfilt, zfilt)
    reactions += {
      case ButtonClicked(c) => c match {
        case `xminus` =>
          xcolslow = xcolslow - 1
          xcolslabel.text = xcolslabeltext
        case `xplus` =>
          xcolslow = xcolslow + 1
          xcolslabel.text = xcolslabeltext
        case `yminus` =>
          ycolslow = ycolslow - 1
          ycolslabel.text = ycolslabeltext
          yfilt.peer.setModel(ComboBox.newConstantModel(yfiltlist))
        case `yplus` =>
          ycolslow = ycolslow + 1
          ycolslabel.text = ycolslabeltext
          yfilt.peer.setModel(ComboBox.newConstantModel(yfiltlist))
        case `zminus` =>
          zcolslow = zcolslow - 1
          zcolslabel.text = zcolslabeltext
          zfilt.peer.setModel(ComboBox.newConstantModel(zfiltlist))
        case `zplus` =>
          zcolslow = zcolslow + 1
          zcolslabel.text = zcolslabeltext
          zfilt.peer.setModel(ComboBox.newConstantModel(zfiltlist))
      }
    }
  }
  ui.visible = true
}

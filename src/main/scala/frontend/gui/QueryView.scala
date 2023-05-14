package frontend.gui

import core.DataCube
import frontend.schema.encoders.ColEncoder
import frontend.schema.{BD2, Dim2, LD2, Schema2}
import org.jfree.chart.axis.{NumberAxis, SymbolAxis}
import org.jfree.chart.plot.{PlotOrientation, XYPlot}
import org.jfree.chart.renderer.xy.DeviationRenderer
import org.jfree.chart.{ChartFactory, ChartPanel}
import org.jfree.data.xy.{DefaultXYDataset, XYSeries, XYSeriesCollection, YIntervalSeries}
import org.jfree.ui.RectangleInsets
import util.BitUtils.permute_bits
import util.Util

import java.awt.{BasicStroke, Color}
import javax.swing.ListSelectionModel
import javax.swing.table.AbstractTableModel
import scala.collection.mutable.ArrayBuffer
import scala.swing.BorderPanel.Position._
import scala.swing.Dialog.{Message, Options, Result}
import scala.swing.ListView.Renderer
import scala.swing._
import scala.swing.event.EditDone

case class QueryView(sch: Schema2, dc: DataCube) {
  val Xdims = collection.mutable.ArrayBuffer[DimHierarchy]()
  val Ydims = collection.mutable.ArrayBuffer[DimHierarchy]()
  val slicedims = collection.mutable.ArrayBuffer[DimHierarchy]()

  def genQuery = {
    val xbits = Xdims.map { i => i.currentLevel.selectedBits }
    val ybits = Ydims.map { i => i.currentLevel.selectedBits }
    val slicebits = slicedims.map { i =>
      val idx = i.currentLevel.selectedValues.head //TODO: Only taking first value. Extend to handle multiple values
      val bits = i.currentLevel.selectedBits
      assert(idx < (1 << bits.size))
      var idx2 = idx
       bits.map { b => //LSB to MSB
        val v = idx2 & 1
        idx2 >>= 1
        b -> v
      }
    }
    val bX = xbits.flatten.size
    val bY = ybits.flatten.size

    val q_unsorted = (xbits.flatten ++ ybits.flatten ++ slicebits.flatten.map(_._1))
    val q_sorted = q_unsorted.sorted

    val perm = q_sorted.map(b => q_unsorted.indexOf(b)).toArray
    val permf = permute_bits(q_unsorted.size, perm)

    val slice = slicebits.flatten.map { case (i, v) => q_sorted.indexOf(i) -> v }.sortBy(_._1)

    val permBackqY = ybits.flatten.sorted.map(b => ybits.flatten.indexOf(b)).toArray
    val permfBackqY = permute_bits(ybits.flatten.size, permBackqY)
    val permBackqX = xbits.flatten.sorted.map(b => xbits.flatten.indexOf(b)).toArray
    val permfBackqX = permute_bits(xbits.flatten.size, permBackqX)

    val numRows = 1 << bY
    val numCols = 1 << bX

    val genSeries = (src: Array[Double]) => {
      val series = Array.fill(numRows)(Array.fill(numCols)(0.0)) //one series per row
      (0 until numRows).foreach { i =>
        (0 until numCols).foreach { j =>
          series(i)(j) = src(permf(j * numRows + i))
        }
      }
      series
    }

    val Xlabels = Xdims.map{ i =>
      val validValues = i.currentLevel.values
      val numInvalid = (1 << i.currentLevel.numBits)-validValues.size
      validValues ++ (1 to numInvalid).map(i => "(invalid)")
    }.foldLeft(Vector[String]("")){ case (acc, cur) => acc.flatMap(x => cur.map(y => x + y))}
    assert(Xlabels.size == numCols, s"Xlabels should be of size $numCols but is ${Xlabels.size}")
    val Ylabels = Ydims.map { i =>
      val validValues = i.currentLevel.values
      val numInvalid = (1 << i.currentLevel.numBits) - validValues.size
      validValues ++ (1 to numInvalid).map(i => "(invalid)")
    }.foldLeft(Vector[String]("")) { case (acc, cur) => acc.flatMap(x => cur.map(y => x + y)) }
    assert(Ylabels.size == numRows, s"Ylabels should be of size $numCols but is ${Ylabels.size}")
    val permutedXLabels = new Array[String](numCols)
    val permutedYLabels = new Array[String](numRows)
    (0 until numCols).foreach{j => permutedXLabels(permfBackqX(j)) = Xlabels(j)}
    (0 until numRows).foreach{i => permutedYLabels(permfBackqY(i)) = Ylabels(i)}
    //TODO: Remove invalid rows and columns
    (q_sorted, slice, genSeries, permutedXLabels, permutedYLabels)
  }
  sch.initBeforeDecode()
  def decodePrefix(enc: ColEncoder[_], numBits: Int): IndexedSeq[String] = {
    if (numBits == 0) {
      Vector("(all)")
    }
    else if (numBits == enc.bits.length) {
      (0 to enc.maxIdx).map(i => enc.decode_locally(i).toString)
    } else {
      val droppedBits = enc.bits.length - numBits
      (0 to enc.maxIdx).groupBy(_ >> droppedBits).map { case (grp, idxes) =>
        val first = enc.decode_locally(idxes.head).toString
        val last = enc.decode_locally(idxes.last).toString
        first + " to " + last
      }.toVector
    }
  }

  case class DimLevel(name: String, enc: ColEncoder[_], var numBits: Int, var selectedValues: Seq[Int]) {
    def values = decodePrefix(enc, numBits)
    def fullName = {
      val grp = if (numBits == bits.size) ""
      else "/" + (1 << (bits.size - numBits))
      name + grp
    }
    val bits = enc.bits
    def selectedBits = bits.takeRight(numBits)
  }

  case class DimHierarchy(levels: IndexedSeq[DimLevel], var levelIdx: Int) {
    def currentLevel = levels(levelIdx)
    def increment = {
      if (currentLevel.numBits < currentLevel.bits.length)
        currentLevel.numBits += 1
      else {
        if (levelIdx < levels.size - 1) {
          levelIdx += 1
          currentLevel.numBits = 0
        }
      }
    }
    def decrement = {
      if (currentLevel.numBits > 0) {
        currentLevel.numBits -= 1
      } else {
        if (levelIdx > 0) {
          levelIdx -= 1
          currentLevel.numBits = currentLevel.bits.size
        }
      }
    }
  }

  def rec(dim: Dim2): Vector[DimHierarchy] = dim match {
    case BD2(name, children, cross) => if (cross) {
      children.map(c => rec(c)).reduce(_ ++ _)
    } else {
      //assuming all children are LD2 here
      val levels = children.map { case LD2(name, encoder) =>
        DimLevel(name, encoder, 0, Nil)
      }
      val hierarchy = DimHierarchy(levels, 0)
      Vector(hierarchy)
    }
    case LD2(name, encoder) =>
      val level = DimLevel(name, encoder, 0, Nil)
      val hierarchy = DimHierarchy(Vector(level), 0)
      Vector(hierarchy)
  }
  val topLevels = rec(sch.root)

  val ui = new MainFrame {
    title = "Query View"
    preferredSize = new Dimension(800, 600)


    val chart = {
      val chart = ChartFactory.createXYLineChart("Measure", "Horizontal", "Vertical", new XYSeriesCollection(), PlotOrientation.VERTICAL, true, false, false)
      chart.setBackgroundPaint(Color.white)
      val plot = chart.getPlot.asInstanceOf[XYPlot]
      plot.setBackgroundPaint(Color.white)
      plot.setAxisOffset(new RectangleInsets(5.0, 5.0, 5.0, 5.0))
      plot.setDomainGridlinePaint(Color.white)
      plot.setRangeGridlinePaint(Color.white)
      chart
    }
    val outputPanel = new BorderPanel {
      peer.add(new ChartPanel(chart))
    }
    lazy val controlPanel = new GridPanel(1, 4) {
      //preferredSize = new Dimension(1920, 300)
      class MyButton(text0: String)(op: => Unit) extends Button(Action(text0)(op)) {
        preferredSize = new Dimension(40, 20)
      }

      case class AggregationDimensionPanel(str: String, dims: ArrayBuffer[DimHierarchy]) extends BorderPanel {
        layout += new Label(str) -> North
        layout += new BorderPanel {
          val dimlist = new ListView(dims) {
            renderer = Renderer(d => d.currentLevel.fullName)
            peer.setSelectionMode(ListSelectionModel.SINGLE_SELECTION)
          }
          layout += new ScrollPane(dimlist) {
            preferredSize = new Dimension(100, 100)
          } -> Center
          layout += new GridPanel(3, 2) {
            contents += new MyButton("+")({
              val addDimComboBox = new ComboBox(topLevels) {
                renderer = Renderer(x => x.levels.head.name)
              }
              val pane = new BorderPanel {
                layout += addDimComboBox -> Center
              }
              Dialog.showConfirmation(contents.head, pane.peer, "Choose dimension to drill down", Options.OkCancel, Message.Plain) match {
                case Result.Ok =>
                  val i = addDimComboBox.selection.index
                  if (i >= 0) {
                    dims += topLevels(i) //TODO: Check whether it already exists
                    dimlist.listData = dims
                  }
                case Result.Cancel => ()
              }
            })

            contents += new MyButton("-")({
              dimlist.selection.indices.foreach(i => dims.remove(i))
              dimlist.listData = dims
            })
            contents += new MyButton("<")({
              val i = dimlist.selection.leadIndex
              if (i >= 0) {
                val d = dims(i)
                d.decrement
                dimlist.listData = dims
                dimlist.selectIndices(i)
              }
            })
            contents += new MyButton(">")({
              val i = dimlist.selection.leadIndex
              if (i >= 0) {
                val d = dims(i)
                d.increment
                dimlist.listData = dims
                dimlist.selectIndices(i)
              }
            })
            contents += new MyButton("^")({
              val i = dimlist.selection.leadIndex
              if (i > 0) {
                val temp = dims(i)
                dims(i) = dims(i - 1)
                dims(i - 1) = temp
              }
              dimlist.listData = dims
              dimlist.selectIndices(i - 1)
            })
            contents += new MyButton("v")({
              val i = dimlist.selection.leadIndex
              if (i < dims.size - 1) {
                val temp = dims(i)
                dims(i) = dims(i + 1)
                dims(i + 1) = temp
              }
              dimlist.listData = dims
              dimlist.selectIndices(i + 1)
            })
          } -> East
        } -> Center

      }

      contents +=  AggregationDimensionPanel("Horizontal Axis", Xdims)
      contents +=  AggregationDimensionPanel("Vertical Axis", Ydims)
      contents += new BorderPanel {
        layout += new Label("Filters") -> North
        layout += new BorderPanel {
          val tabModel = new AbstractTableModel {
            override def getColumnName(column: Int): String = " "
            def getRowCount: Int = slicedims.length
            def getColumnCount: Int = 3
            def getValueAt(row: Int, col: Int): AnyRef = {
              val r = slicedims(row)
              col match {
                case 0 => r.currentLevel.fullName
                case 1 => "="
                case 2 => r.currentLevel.selectedValues.map { i => r.currentLevel.values(i) }.mkString(";")
                case _ => "???"
              }
            }
            override def isCellEditable(row: Int, column: Int) = false
          }
          val table = new Table(tabModel) {
            peer.setSelectionMode(ListSelectionModel.SINGLE_SELECTION)
            peer.getTableHeader.setReorderingAllowed(false)
            peer.getColumnModel.getColumn(0).setPreferredWidth(20)
            peer.getColumnModel.getColumn(1).setMaxWidth(20)
          }
          layout += new ScrollPane(table) {
            preferredSize = new Dimension(100, 100)
          } -> Center
          layout += new GridPanel(3, 1) {
            contents += new MyButton("+")({
              val addDimComboBox = new ComboBox(topLevels) {
                renderer = Renderer(x => x.levels.head.name)
              }
              val pane = new BorderPanel {
                layout += addDimComboBox -> Center
              }
              Dialog.showConfirmation(contents.head, pane.peer, "Choose dimension to slice", Options.OkCancel, Message.Plain) match {
                case Result.Ok =>
                  val i = addDimComboBox.selection.index
                  if (i >= 0) {
                    slicedims += topLevels(i) //TODO: Check whether it already exists
                    tabModel.fireTableRowsInserted(slicedims.size - 1, slicedims.size - 1)
                  }
                case Result.Cancel => ()
              }
            })
            contents += new MyButton("-")({
              val i = table.selection.rows.leadIndex
              slicedims.remove(i)
              tabModel.fireTableRowsDeleted(i, i)
            })
            contents += new MyButton("=")({
              val i = table.selection.rows.leadIndex
              val d = slicedims(i)
              val valuepane = new BorderPanel {
                val searchBar = new TextField(20)
                var checkboxes = d.currentLevel.values.map(v => v -> new CheckBox(v))
                val checkboxDisplay = new BoxPanel(Orientation.Vertical) {
                  contents ++= checkboxes.filter(_._1.contains(searchBar.text)).map(_._2)
                }
                layout += new BoxPanel(Orientation.Vertical) {
                  contents += new FlowPanel {
                    val slicelabel = new Label(d.currentLevel.fullName)
                    contents += slicelabel
                    contents += new MyButton("<")({
                      d.decrement
                      slicelabel.text = d.currentLevel.fullName
                      checkboxes = d.currentLevel.values.map(v => v -> new CheckBox(v))
                      checkboxDisplay.contents.clear()
                      checkboxDisplay.contents ++= checkboxes.filter(_._1.contains(searchBar.text)).map(_._2)
                      checkboxDisplay.revalidate()
                      scrollbar.contents = checkboxDisplay
                    })
                    contents += new MyButton(">")({
                      d.increment
                      slicelabel.text = d.currentLevel.fullName
                      checkboxes = d.currentLevel.values.map(v => v -> new CheckBox(v))
                      checkboxDisplay.contents.clear()
                      checkboxDisplay.contents ++= checkboxes.filter(_._1.contains(searchBar.text)).map(_._2)
                      checkboxDisplay.revalidate()
                      scrollbar.contents = checkboxDisplay
                    })
                  }
                  contents += searchBar
                } -> North

                val scrollbar = new ScrollPane(checkboxDisplay) {
                  preferredSize = new Dimension(100, 100)
                }
                layout += scrollbar -> Center
                listenTo(searchBar)
                reactions += {
                  case EditDone(r) =>
                    checkboxDisplay.contents.clear()
                    checkboxDisplay.contents ++= checkboxes.filter(_._1.contains(searchBar.text)).map(_._2)
                    checkboxDisplay.revalidate()
                    scrollbar.contents = checkboxDisplay
                }
              }
              Dialog.showMessage(contents.head, valuepane.peer, "Edit Slice Values", messageType = Message.Plain)
              d.currentLevel.selectedValues = valuepane.checkboxes.indices.filter { i => valuepane.checkboxes(i)._2.selected }
              tabModel.fireTableCellUpdated(i, 2)
            })
          } -> East
        } -> Center
      }
      contents += new BoxPanel(Orientation.Vertical) {
        contents += new Label("Measure")
        contents += new ComboBox(List("Sales"))
        contents += new Label("Solver")
        contents += new ComboBox(List("Naive", "LPP", "Moment", "IPF"))
        contents += Button("Run")({
          val (q, slice, seriesGen, xlabels, ylabels) = genQuery
          val fullresult = dc.naive_eval(q)
          val result =  Util.slice(fullresult, slice)
          val seriesData = seriesGen(result)
          val dataset = new XYSeriesCollection()
          seriesData.indices.foreach{ i =>
            val s = new XYSeries(ylabels(i))
            println(ylabels(i) + " :: "  + seriesData(i).mkString(" "))
            seriesData(i).zipWithIndex.foreach{ case(y, x) => s.add(x, y)}
            dataset.addSeries(s)
          }
          val plot = chart.getXYPlot
            plot.setDataset(dataset)
          chart.setTitle("CurrentMeasureName")
          val xaxis =  new SymbolAxis("CurrentXLabel", xlabels)
          plot.setDomainAxis(xaxis)
          val yaxis = new NumberAxis("CurrentYLabel")
          plot.setRangeAxis(yaxis)
          chart.fireChartChanged()
        })
      }
    }
    val mainPanel = new BorderPanel {
      layout(outputPanel) = Center
      layout(controlPanel) = South
    }
    override def closeOperation(): Unit = {
      //SVGGenerator.save(controlPanel, "controlpanel.svg")
      //println("Saved file")
      super.closeOperation()
    }
    contents = mainPanel
  }
  ui.visible = true

}

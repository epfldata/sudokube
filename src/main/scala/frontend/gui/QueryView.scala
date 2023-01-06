package frontend.gui

import javax.swing.{JPanel, ListSelectionModel}
import javax.swing.table.AbstractTableModel
import scala.collection.mutable.ArrayBuffer
import scala.swing.BorderPanel.Position._
import scala.swing.Dialog.Message
import scala.swing.ListView.Renderer
import scala.swing.Swing.EmptyIcon
import scala.swing._
import scala.swing.event.{ButtonClicked, EditDone}

case class QueryView() {

  val ui = new MainFrame {
    title = "Query View"
    preferredSize = new Dimension(800, 600)
    contents = mainPanel

    lazy val mainPanel = new BorderPanel {
      layout(outputPanel) = Center
      layout(controlPanel) = South
    }

    lazy val outputPanel = new Table(10, 10) {
      showGrid = true
    }
    lazy val controlPanel = new GridPanel(1, 3) {
      //preferredSize = new Dimension(1920, 300)
      class MyButton(text0: String)(op: => Unit) extends Button(Action(text0)(op)) {
        preferredSize = new Dimension(40, 40)
      }

      case class AggregationDimension(name: String, bits: IndexedSeq[Int], var numBits: Int)

      case class SliceDimension(name: String, bits: IndexedSeq[Int], var numBits: Int, var selectedEntries: Seq[(String, Int)])

      case class CosmeticDimension(name: String, bits: IndexedSeq[Int], values: IndexedSeq[String])

      val allDims = (1 to 10).map(i => CosmeticDimension("Dim" + i, i * 7 until (i + 1) * 7, (0 until 1 << 7).map { j => "V" + j }))

      case class AggregationDimensionPanel(str: String) extends BorderPanel {
        layout += new Label(str) -> North
        layout += new BorderPanel {
          val dims = collection.mutable.ArrayBuffer[AggregationDimension]()
          val dimlist = new ListView(dims) {
            renderer = Renderer(d => d.name + s": ${d.numBits}/${d.bits.length}")
            peer.setSelectionMode(ListSelectionModel.SINGLE_SELECTION)
          }
          layout += new ScrollPane(dimlist) -> Center
          layout += new GridPanel(3, 2) {
            contents += new MyButton("+")({
              val res = Dialog.showInput(contents.head,
                "Choose dimension to drill down on",
                title = "Add dimensions", entries = allDims.map(_.name), initial = 0, icon = EmptyIcon, messageType = Message.Plain)
              res match {
                case None => ()
                case Some(dimname) =>
                  val dim = allDims.find(_.name == dimname).get
                  dims += AggregationDimension(dim.name, dim.bits, 0)
                  dimlist.listData = dims
              }
            })

            contents += new MyButton("-")({
              dimlist.selection.indices.foreach(i => dims.remove(i))
              dimlist.listData = dims
            })
            contents += new MyButton("<")({
              val i = dimlist.selection.leadIndex
              if (i > 0) {
                val d = dims(i)
                if (d.numBits > 0) d.numBits -= 1
                dimlist.listData = dims
                dimlist.selectIndices(i)
              }
            })
            contents += new MyButton(">")({
              val i = dimlist.selection.leadIndex
              if (i > 0) {
                val d = dims(i)
                if (d.numBits < d.bits.length) d.numBits += 1
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

      contents ++= List("Horizontal", "Vertical").map(s => AggregationDimensionPanel(s))
      contents += new BorderPanel {
        layout += new Label("Slice") -> North
        layout += new BorderPanel {
          val dims = collection.mutable.ArrayBuffer[SliceDimension]()
          val tabModel = new AbstractTableModel {
            override def getColumnName(column: Int): String = " "
            def getRowCount: Int = dims.length
            def getColumnCount: Int = 3
            def getValueAt(row: Int, col: Int): AnyRef = {
              val r = dims(row)
              col match {
                case 0 => r.name + s": ${r.numBits}/${r.bits.length}"
                case 1 => "="
                case 2 => r.selectedEntries.map(_._1).mkString(";")
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
              val res = Dialog.showInput(contents.head,
                "Choose dimension to slice",
                title = "Add dimensions", entries = allDims.map(_.name), initial = 0, icon = EmptyIcon, messageType = Message.Plain)
              res match {
                case None => ()
                case Some(dimname) =>
                  val dim = allDims.find(_.name == dimname).get
                  dims += SliceDimension(dim.name, dim.bits, 0, dim.values.zipWithIndex)
                  tabModel.fireTableRowsInserted(dims.size - 1, dims.size - 1)
              }

            })
            contents += new MyButton("-")({
              val i = table.selection.rows.leadIndex
              dims.remove(i)
              tabModel.fireTableRowsDeleted(i, i)
            })
            contents += new MyButton("=")({
              val i = table.selection.rows.leadIndex
              val i2 = if (i > 0) i else 0 //TODO: Find index of selection among all dimensions
              val valuepane = new BorderPanel {
                val searchBar = new TextField(20)
                layout += new FlowPanel {
                  contents += searchBar
                  contents += new MyButton(">")()
                  contents += new MyButton("<")()
                } -> North
                val checkboxes = allDims(i2).values.map(v => v -> new CheckBox(v))
                val checkboxDisplay = new BoxPanel(Orientation.Vertical) {
                  contents ++= checkboxes.filter(_._1.contains(searchBar.text)).map(_._2)
                }
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
              dims(i).selectedEntries = valuepane.checkboxes.zipWithIndex.filter{case ((v, b), i) => b.selected}.map{case ((v, b), i) => (v, i)}
              tabModel.fireTableCellUpdated(i, 2)
            })
          } -> East
        } -> Center
      }
    }
  }

  ui.visible = true
}

package frontend.gui

import javax.swing.table.AbstractTableModel
import scala.collection.mutable.ArrayBuffer
import scala.swing.BorderPanel.Position._
import scala.swing.Dialog.{Message, Options, Result}
import scala.swing.ListView.Renderer
import scala.swing._
import scala.util.{Random, Try}

class MaterializationView {
  val ui = new MainFrame {
    title = "Materialization View"
    preferredSize = new Dimension(800, 600)
    contents = mainPanel
    lazy val mainPanel = new BorderPanel {

      //actual global bits
      case class DisplayedCuboids(bits: Map[String, Set[Int]]) {
        val allbits = bits.values.foldLeft(Set[Int]())(_ ++ _)
        def isMaterialized: Boolean = {
          materializedCuboids.contains(allbits)
        }
      }


      val materializedCuboids = collection.mutable.Set[Set[Int]]()

      val myheader = ArrayBuffer[(String, Seq[Int])]()
      myheader += "Country" -> (0 to 5)
      myheader += "City" -> (5 to 10)
      myheader += "Year" -> (10 to 15)
      myheader += "Month" -> (15 to 19)
      myheader += "Day" -> (19 to 24)

      lazy val allCuboids = (1 to 1000).map { i =>
        val numCols = Random.nextInt(myheader.size)
        val cols = util.Util.collect_n(numCols, () => myheader(Random.nextInt(myheader.size)))
        if (i < 10) println(cols)
        DisplayedCuboids(cols.map { case (cn, cbits) =>
          val nbits = Random.nextInt(cbits.size)
          cn -> cbits.sorted.takeRight(nbits).toSet
        }.toMap)
      }

      case class FilterRange(idx: Int, localRange: Range) {
        val cname = myheader(idx)._1
        val allbits = myheader(idx)._2.sorted.toIndexedSeq
        val selectedBits = localRange.map { i => allbits(i) }.toSet
      }

      def cuboidSatisfiesFilter(cub: DisplayedCuboids) = {
        filterBits.foldLeft(true) { case (acc, cur) =>
          acc && cur.selectedBits.subsetOf(cub.allbits)
        }
      }
      val filterBits = ArrayBuffer[FilterRange]()
      val displayedCuboids = ArrayBuffer[DisplayedCuboids]()

      def generateToDisplay(): Unit = {

        displayedCuboids.clear()
        displayedCuboids ++= allCuboids.iterator.filter(cuboidSatisfiesFilter).take(20)
        tabModel.fireTableDataChanged()
      }

      val tabModel = new AbstractTableModel {

        override def getColumnName(column: Int): String = if (column > 0) myheader(column - 1)._1 + "(" + myheader(column - 1)._2.size + " bits)" else "Materialize?"
        override def getRowCount: Int = displayedCuboids.size
        override def getColumnCount: Int = myheader.size + 1
        override def setValueAt(aValue: Any, rowIndex: Int, columnIndex: Int): Unit = {
          assert(columnIndex == 0)
          val bool = aValue.asInstanceOf[Boolean]
          if (bool) materializedCuboids += displayedCuboids(rowIndex).allbits
          else materializedCuboids -= displayedCuboids(rowIndex).allbits
        }
        override def getValueAt(rowIndex: Int, columnIndex: Int) = columnIndex match {
          case 0 => displayedCuboids(rowIndex).isMaterialized.asInstanceOf[AnyRef]
          case id =>
            val (colname, allbits) = myheader(columnIndex - 1)
            val cuboidBits = displayedCuboids(rowIndex).bits.getOrElse(colname, Set())
            allbits.sorted.reverse.map { b => if (cuboidBits contains b) "\u2589" else "\u25A2" }.mkString
        }
        override def isCellEditable(rowIndex: Int, columnIndex: Int): Boolean = columnIndex == 0
      }

      generateToDisplay()
      layout += new ScrollPane {
        contents = new Table(tabModel) {
          peer.setRowSelectionAllowed(false)
          peer.setColumnSelectionAllowed(false)
        }
        preferredSize = new Dimension(800, 400)
      } -> Center
      val listView = new ListView(filterBits) {
        renderer = Renderer(x => x.cname + "[" + x.localRange.head + "-" + x.localRange.last + "]")
      }
      layout += new BorderPanel {
        layout += new FlowPanel {
          contents += Button("+") {
            val addDimComboBox = new ComboBox(myheader) {
              renderer = Renderer(x => x._1)
            }
            val fieldSize = new Dimension(20, 20)
            val b0 = new TextField() {
              preferredSize = fieldSize
            }
            val b1 = new TextField() {
              preferredSize = fieldSize
            }
            val pane = new FlowPanel() {
              contents += addDimComboBox
              contents += b0
              contents += b1
            }
            Dialog.showConfirmation(contents.head, pane.peer, "Choose columns to filter", Options.OkCancel, Message.Plain) match {
              case Result.Ok =>
                val i = addDimComboBox.selection.index
                val s0 = Try { b0.text.toInt }.toOption
                val s1 = Try { b1.text.toInt }.toOption
                val d = myheader(i)
                if (i >= 0 && s0.isDefined && s1.isDefined) {
                  val i0 = s0.get
                  val i1 = s1.get
                  if (i0 >= 0 && i1 < d._2.size && i1 >= i0) {
                    filterBits += FilterRange(i, i0 to i1)
                    listView.listData = filterBits
                    generateToDisplay()
                  }
                }
              case Result.Cancel => ()
            }
          }
          contents += Button("-") {
            val idx = listView.selection.leadIndex
            if (idx >= 0) {
              filterBits.remove(idx)
              listView.listData = filterBits
              generateToDisplay()
            }
          }
        } -> North
        layout += new ScrollPane(listView) -> Center
      } -> West
    }
  }
  ui.visible = true
}

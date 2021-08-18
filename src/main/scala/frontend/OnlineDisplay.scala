//package ch.epfl.data.sudokube
package frontend
import schema._
import core._
import scala.swing._


case class OnlineDisplay(schema: Schema, dc: DataCube,
  draw: (Schema, List[Int], Seq[Interval[Rational]]) => String
) {

  // this is slow!
  val ui = new MainFrame {
    title = "sudokube"
    preferredSize = new Dimension(600, 400)
    val ta = new TextArea("") {
      font = new Font("Courier", 0, 20)
    }
    contents = ta
  }

  ui.visible = true

  var last_s : Option[SparseSolver[Rational]] = None

  protected var ex = false // set to true to exit from thread

  /** runs in the main thread */
  def l_run(q: List[Int], cheap_size: Int) = {
    import RationalTools._

    def callback(s: SparseSolver[Rational]) = {
      s.propagate_bounds(0 to s.n_vars - 1)
      ui.ta.text = draw(schema, q, s.bounds)
      last_s = Some(s)
      (! ex) // keep going?
    }

    dc.online_agg[Rational](q, cheap_size, callback)
  }

  /** runs in a separate thread */
  def t_run(q: List[Int], cheap_size: Int) {
    ex = false
    (new Thread { override def run { l_run(q, cheap_size) } }).start
  }

  /** stops the separate thread, but only at the end of an iteration. */
  def stop { ex = true }

} // end OnlineDisplay


/*
import core._
import frontend._
import frontend.schema._
val dc  = experiments.Tools.mkDC(60, .1, 1.1, 100)
val sch = StaticSchema.mk(60)
val od  = OnlineDisplay(sch, dc, PrettyPrinter.formatPivotTable)

od.l_run(List(0,1,2,3), 4)
//od.ui.visible = false

*/



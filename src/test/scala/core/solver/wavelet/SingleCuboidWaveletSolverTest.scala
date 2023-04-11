package core.solver.wavelet

import org.scalatest.FunSuite
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.prop.TableFor3

class SingleCuboidWaveletSolverTest extends FunSuite {

  val cuboids: TableFor3[Seq[Int], Array[Double], Array[Double]] = Table(
    ("variables", "cuboid", "expected"),
    (Seq(0, 1, 2), Array(1.0, 19.0, 13.0, 1.0, 4.0, 7.0, 1.0, 13.0), Array(1.0, 19.0, 13.0, 1.0, 4.0, 7.0, 1.0, 13.0)),
    (Seq(0, 1), Array(5, 26.0, 14.0, 14.0), Array(2.5, 13.0, 7.0, 7.0, 2.5, 13.0, 7.0, 7.0)),
    (Seq(0, 2), Array(14.0, 20.0, 5.0, 20.0), Array(7.0, 10.0, 7.0, 10.0, 2.5, 10.0, 2.5, 10.0))
  )

  forAll(cuboids) { (variables, cuboid, expected) =>
    val solver = new SingleCuboidWaveletSolver(3)
    solver.addCuboid(variables, cuboid)

    val result = solver.solve()

    assert(result sameElements expected)
  }
}

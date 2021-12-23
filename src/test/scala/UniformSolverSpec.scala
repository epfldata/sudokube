import org.scalatest._
import core._
import SolverTools._
import backend.Payload
import core.solver.UniformSolver
import planning.ProjectionMetaData
import RationalTools._

class UniformSolverSpec extends FlatSpec with Matchers {

  "UniformSolver " should " work when full cuboid is known using fast solve " in {
    val cuboid =  Array(1, 3, 2, 1, 5, 1, 0, 2).map(_.toDouble)
    val solver = new UniformSolver[Double](3)
    solver.add(List(0,1,2), cuboid)
    solver.fillMissing()
    solver.fastSolve()
    assert(cuboid.sameElements(solver.solution))
  }
}

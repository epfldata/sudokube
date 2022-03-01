import org.scalatest._
import core._
import SolverTools._
import backend.Payload
import core.solver.MomentSolverAll
import planning.ProjectionMetaData
import RationalTools._

class MomentSolverAllSpec extends FlatSpec with Matchers {

  "MomentSolver " should " work when full cuboid is known using fast solve " in {
    val cuboid =  Array(1, 3, 2, 1, 5, 1, 0, 2).map(_.toDouble)
    val solver = new MomentSolverAll[Double](3)
    solver.add(List(0,1,2), cuboid)
    solver.fillMissing()
    solver.fastSolve()
    assert(cuboid.sameElements(solver.solution))
  }
}

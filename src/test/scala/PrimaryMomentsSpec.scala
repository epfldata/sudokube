import backend.CBackend
import core.solver.SolverTools
import core.solver.moment.Moment1Transformer
import frontend.Sampling
import frontend.experiments.Tools
import frontend.experiments.Tools.mkDC
import org.scalatest.{FlatSpec, Matchers, color}

import java.io.File
import scala.reflect.io.Directory

class PrimaryMomentsSpec extends FlatSpec with Matchers {

  "Primary Moments" should "match naive eval " in {
    implicit val backend = CBackend.default
    val nbits = 13
    val qsize = 5

    val dc = mkDC(nbits, 1, 1.5, 4096, Sampling.f1, backend)
    val (t1, m1) = SolverTools.primaryMoments(dc)

    val m2 = Array.fill(nbits)(0L)
    val t2 = dc.naive_eval(Vector(1)).sum.toLong

    m2.indices.foreach(i => m2(i) = dc.naive_eval(Vector(i))(1).toLong)
    assert(t1 == t2)
    assert(m1.sameElements(m2))
    val cubename = "testprimarymoment"
    val dir = new Directory(new File(s"cubedata/$cubename"))
    if (!dir.exists) dir.createDirectory()
    dc.primaryMoments = (t1, m1)
    dc.savePrimaryMoments(cubename)

    val dc2 = new core.DataCube()
    dc2.loadPrimaryMoments(cubename)
    val (t3, m3) = dc2.primaryMoments

    assert(t1 == t3)
    assert(m1.sameElements(m3))

    val q = Tools.rand_q(nbits, qsize)
    val m4 = SolverTools.preparePrimaryMomentsForQuery[Double](q, (t1, m1))

    val naiveres = dc.naive_eval(q)
    val m5 = Moment1Transformer[Double]().getMoments(naiveres)
    m4.foreach{
      case (0, t) => assert(t == t1)
      case (i , m) => assert(m5(i) == m)
    }
    dir.deleteRecursively()
  }
}

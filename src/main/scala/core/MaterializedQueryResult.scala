package core

import backend.CBackend
import frontend.experiments.Tools
import frontend.generators.{CubeGenerator, NYC, RandomCubeGenerator, SSB, SSBSample}
import util.ProgressIndicator

import java.io.{File, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

class MaterializedQueryResult(cg: CubeGenerator, isPrefix: Boolean = true) {
  lazy val sch = cg.schemaInstance
  var baseCube: DataCube = null
  val folder = if(isPrefix) "prefix" else "random"
  def ensureLoadBase() = if (baseCube == null)
    baseCube = cg.loadBase()

  def generateAndSaveQueries(nq: Int, qs: Int) = {
    println(s"Generating $nq $folder queries of length $qs for ${cg.inputname} dataset")
    val queries = (0 until nq).map { i => Tools.generateQuery(isPrefix, sch, qs)}.toVector
    val queryFile = new File(s"cubedata/${cg.inputname}_queries/$folder/Q$qs/queries.bin")
    if (!queryFile.exists())
      queryFile.getParentFile.mkdirs()
    val queryOut = new ObjectOutputStream(new FileOutputStream(queryFile))
    queryOut.writeObject(queries)
    queryOut.close()

    //write results
    ensureLoadBase()
    val pi = new ProgressIndicator(queries.size, "Materializing query result ")
    queries.indices.foreach { queryIdx =>
      val q = queries(queryIdx)
      val qres = baseCube.naive_eval(q)
      val resFile = new File(s"cubedata/${cg.inputname}_queries/$folder/Q$qs/q${queryIdx}.bin")
      val resOut = new ObjectOutputStream(new FileOutputStream(resFile))
      resOut.writeObject(qres)
      pi.step
    }
  }

  def loadQueries(qs: Int) = {
    val queryFile = new File(s"cubedata/${cg.inputname}_queries/$folder/Q$qs/queries.bin")
    val queryIn = new ObjectInputStream(new FileInputStream(queryFile))
    queryIn.readObject().asInstanceOf[Vector[Vector[Int]]]
  }

  def loadQueryResult(qs: Int, queryIdx: Int) = {
    val resFile = new File(s"cubedata/${cg.inputname}_queries/$folder/Q$qs/q${queryIdx}.bin")
    val resIn = new ObjectInputStream(new FileInputStream(resFile))
    resIn.readObject().asInstanceOf[Array[Double]]
  }

}

object MaterializedQueryResult {
  def main(args: Array[String]) {
    implicit val backend = CBackend.default
    val nq = 100
    val qss = List(6, 10, 14)//List(2, 4)//List(8, 10, 14)//List(6, 9, 12, 15, 18, 21, 24)

    val isPrefix = args.lift(0).map(_.toBoolean).getOrElse(true)
    val cg = args(1) match {
      case "ssbsample" => new SSBSample(20)
      case "random" => new RandomCubeGenerator(100, 20)
      case "ssb" => new SSB(100)
      case "nyc" => new NYC()
    }
      val mqr = new MaterializedQueryResult(cg, isPrefix)
      qss.foreach { qs =>
        mqr.generateAndSaveQueries(nq, qs)
      }
      backend.reset
  }
}
package core

import backend.CBackend
import frontend.generators.{CubeGenerator, NYC, SSB}

import java.io.{File, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

class MaterializedQueryResult(cg: CubeGenerator) {
  val sch = cg.schemaInstance
  var baseCube: DataCube = null

  def ensureLoadBase() = if (baseCube == null)
    baseCube = cg.loadBase()

  def generateAndSaveQueries(nq: Int, qs: Int) = {
    println(s"Generating $nq queries of length $qs for ${cg.inputname} dataset")
    val queries = (0 until nq).map { i => sch.root.samplePrefix(qs).sorted.toVector }.distinct.toVector
    val queryFile = new File(s"cubedata/${cg.inputname}_queries/Q$qs/queries.bin")
    if (!queryFile.exists())
      queryFile.getParentFile.mkdirs()
    val queryOut = new ObjectOutputStream(new FileOutputStream(queryFile))
    queryOut.writeObject(queries)
    queryOut.close()

    //write results
    ensureLoadBase()
    print("Materializing query result   ")
    queries.indices.foreach { queryIdx =>
      print(queryIdx + "  ")
      val q = queries(queryIdx)
      val qres = baseCube.naive_eval(q)
      val resFile = new File(s"cubedata/${cg.inputname}_queries/Q$qs/q${queryIdx}.bin")
      val resOut = new ObjectOutputStream(new FileOutputStream(resFile))
      resOut.writeObject(qres)
    }
    println()
  }

  def loadQueries(qs: Int) = {
    val queryFile = new File(s"cubedata/${cg.inputname}_queries/Q$qs/queries.bin")
    val queryIn = new ObjectInputStream(new FileInputStream(queryFile))
    queryIn.readObject().asInstanceOf[Vector[Vector[Int]]]
  }

  def loadQueryResult(qs: Int, queryIdx: Int) = {
    val resFile = new File(s"cubedata/${cg.inputname}_queries/Q$qs/q${queryIdx}.bin")
    val resIn = new ObjectInputStream(new FileInputStream(resFile))
    resIn.readObject().asInstanceOf[Array[Double]]
  }

}

object MaterializedQueryResult {
  def main(args: Array[String]): Unit = {
    val SSBQueries = new MaterializedQueryResult(SSB(100))
    val nycQueries = new MaterializedQueryResult(NYC)
    val nq = 100
    val qss = List(6, 9, 12, 15, 18, 21, 24)

    qss.foreach { qs =>
      SSBQueries.generateAndSaveQueries(nq, qs)
    }
    CBackend.b.reset
    qss.foreach { qs =>
      nycQueries.generateAndSaveQueries(nq, qs)
    }
    CBackend.b.reset
  }
}
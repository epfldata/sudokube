package frontend

import backend.CBackend
import core.{DataCube, RandomizedMaterializationScheme2}
import frontend.schema.Schema

class UserCube(val cube: DataCube, val sch : Schema) {


  def save(filename: String): Unit = {
    cube.save2(filename)
    sch.save(filename)
  }

  def query(q: List[(String, Int)]) : Unit = {

  }
}

object UserCube {
  def load(filename: String): UserCube = {
    val cube = DataCube.load2(filename)
    val schema = Schema.load(filename)
    new UserCube(cube, schema)
  }

  def createFromJson(filename: String, fieldToConsider: String): UserCube = {
    val sch = new schema.DynamicSchema
    val R = sch.read(filename, Some(fieldToConsider), _.asInstanceOf[Int].toLong)
    for ((u, v) <- R) {
      println("initial : " + u + ", decodé " + sch.decode_tuple(u) + " : " + v + "\n")
    }
    val matScheme = RandomizedMaterializationScheme2(sch.n_bits, 8, 4, 4)
    val dc = new DataCube(matScheme)
    dc.build(CBackend.b.mk(sch.n_bits, R.toIterator))
    new UserCube(dc, sch)
  }
}

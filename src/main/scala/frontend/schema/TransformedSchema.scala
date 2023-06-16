package frontend.schema

import frontend.schema.encoders.{DynamicColEncoder, MergedMemColEncoder}

import java.io._

class TransformedSchema(override val root: BD2) extends Schema2 {
  override def columnVector: Vector[LD2[_]] = root.leaves
  override def n_bits: Int = columnVector.map {
    case LD2(_, e: DynamicColEncoder[_]) => e.allBits.size
    case LD2(_, e: MergedMemColEncoder[_]) => e.allBits.size
  }.sum

  def save(filename: String): Unit = {
    val file = new File("cubedata/" + filename + "/" + filename + ".sch")
    if (!file.exists())
      file.getParentFile.mkdirs()
    val oos = new ObjectOutputStream(new FileOutputStream(file))
    oos.writeObject(this)
    oos.close()
  }
}

object TransformedSchema {
  def load(filename: String): TransformedSchema = {
    val file = new File("cubedata/" + filename + "/" + filename + ".sch")
    val ois = new ObjectInputStream(new FileInputStream(file)) {
      override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
        try {Class.forName(desc.getName, false, getClass.getClassLoader) }
        catch {case ex: ClassNotFoundException => super.resolveClass(desc)}
      }
    }
    val sch = ois.readObject.asInstanceOf[TransformedSchema]
    ois.close()
    sch
  }
}
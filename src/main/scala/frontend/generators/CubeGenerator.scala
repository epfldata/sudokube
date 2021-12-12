package frontend.generators

import backend.CBackend
import core.{DataCube, MaterializationScheme, RandomizedMaterializationScheme}
import frontend.schema.StructuredDynamicSchema
import util.BigBinary

abstract class CubeGenerator(val inputname: String) {

  def generate() : (StructuredDynamicSchema, Seq[(BigBinary, Long)])
  def save(lrf: Double, lbase: Double) = {
    val (sch, r) = generate()
    val rf = math.pow(10, lrf)
    val base = math.pow(10, lbase)
    val dc = new DataCube(RandomizedMaterializationScheme(sch.n_bits, rf, base))
    sch.save(inputname)
    dc.build(CBackend.b.mkAll(sch.n_bits, r))
    dc.save2(s"${inputname}_${lrf}_${lbase}")
    (sch, dc)
  }

  def saveBase() = {
    val (sch, r) = generate()
    sch.columnVector.map(c => (c.name, c.encoder.isRange, c.encoder.bits.size, c.encoder.bits)).foreach(println)
    val nonRanges = sch.columnVector.filter(c => !c.encoder.isRange)
    if(!nonRanges.isEmpty) {
      println("WARNING ! Encoding not continous for the following ")
      nonRanges.map(c => c.name).foreach(println)
    }
    println("Total = "+sch.n_bits)
    println("Recommended (log) parameters for Materialization schema " + sch.recommended_cube)
    val dc = new DataCube(MaterializationScheme.only_base_cuboid(sch.n_bits))
    sch.save(inputname)
    dc.build(CBackend.b.mkAll(sch.n_bits, r))
    dc.save2(s"${inputname}_base")
    (sch, dc)
  }

  def loadBase() = {
    val sch = StructuredDynamicSchema.load(inputname)
    sch.columnVector.map(c => c.name -> c.encoder.bits).foreach(println)
    println("Total = "+sch.n_bits)
    println("Recommended (log) parameters for Materialization schema " + sch.recommended_cube)
    val dc = DataCube.load2(s"${inputname}_base")
    (sch, dc)
  }
  def buildFromBase(lrf2: Double, lbase2: Double) = {
    val rf2 = math.pow(10, lrf2)
    val base2 = math.pow(10, lbase2)
    val dc1 = DataCube.load2(s"${inputname}_base")
    val dc2 = new DataCube(RandomizedMaterializationScheme(dc1.m.n_bits, rf2, base2))

    dc2.buildFrom(dc1)
    dc2.save2(s"${inputname}_${lrf2}_${lbase2}")
  }
  def multiload(params: Seq[(Double, Double)]) = {
    val sch = StructuredDynamicSchema.load(inputname)
    sch.columnVector.map(c => c.name -> c.encoder.bits).foreach(println)
    val dcs = params.map{p => p -> loadDC(p._1, p._2)}
    (sch, dcs)
  }
  def load2() = {
    val sch = StructuredDynamicSchema.load(inputname)
    sch.columnVector.map(c => c.name -> c.encoder.bits).foreach(println)
    val dc = DataCube.load2(s"${inputname}_sch")
    (sch, dc)
  }
  def load(lrf: Double, lbase: Double) = {
    val sch = StructuredDynamicSchema.load(inputname)
    sch.columnVector.map(c => c.name -> c.encoder.bits).foreach(println)
    println("Total = "+sch.n_bits)
    val dc = DataCube.load2(s"${inputname}_${lrf}_${lbase}")
    (sch, dc)
  }

  def schema() = {
    val sch = StructuredDynamicSchema.load(inputname)
    sch.columnVector.map(c => c.name -> c.encoder.bits).foreach(println)
    println("Total = "+sch.n_bits)
    sch
  }
  def loadDC(lrf: Double, lbase: Double) = {
    DataCube.load2(s"${inputname}_${lrf}_${lbase}")
  }
}

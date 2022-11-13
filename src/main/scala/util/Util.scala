//package ch.epfl.data.sudokube
package util

import backend.CBackend
import core.PartialDataCube

import scala.reflect.ClassTag


class ProgressIndicator(num_steps: Int, name: String = "", showProgress: Boolean = true) {
  private val one_percent = num_steps.toDouble / 100
  private var done = 0
  private val startTime = System.nanoTime()
  if (showProgress) {
    print(name + "  ")
  }

  def step = if (showProgress) {
    this.synchronized {
      done += 1
      if (done % one_percent < 1) print((done / one_percent).toInt + "%")
      if (done == num_steps) {
        val endTime = System.nanoTime()
        val ms = (endTime - startTime) / (1000 * 1000)
        println(s"  took $ms ms")
      }
    }
  }
}


object SloppyFractionalInt {
  implicit object IntOps extends Fractional[Int] {
    def div(x: Int, y: Int): Int = x / y

    // Members declared in scala.math.Numeric
    def fromInt(x: Int): Int = x

    def minus(x: Int, y: Int): Int = x - y

    def negate(x: Int): Int = -x

    def plus(x: Int, y: Int): Int = x + y

    def times(x: Int, y: Int): Int = x * y

    def toDouble(x: Int): Double = x.toDouble

    def toFloat(x: Int): Float = x.toFloat

    def toInt(x: Int): Int = x

    def toLong(x: Int): Long = x.toLong

    // Members declared in scala.math.Ordering
    def compare(x: Int, y: Int): Int = x compare y
  }
}


object Util {
  def slice[T: ClassTag](a: Array[T], slice: Seq[(Int, Int)]) = {
    val allCols = a.length - 1
    val sliceCols = slice.map(x => 1 << x._1).sum
    val sliceInt = slice.map(x => x._2 << x._1).sum
    val aggCols = allCols - sliceCols
    val aggN = a.length >> slice.length
    val result = new Array[T](aggN)
    (0 until aggN).map { i0 =>
      val i = BitUtils.unprojectIntWithInt(i0, aggCols)
      result(i0) = a(i + sliceInt)
    }
    result
  }

  //converts long to type T
  def fromLong[T](long: Long)(implicit num: Fractional[T]): T = {
    val upper = (long >> 31).toInt
    val lower = (long & Int.MaxValue).toInt
    num.plus(num.times(num.fromInt(upper), num.plus(num.fromInt(Int.MaxValue), num.one)), num.fromInt(lower))
  }

  //Displays storage statistics per cuboid size
  def stats(dcname: String, basename: String)(implicit backend: CBackend) = {
    val dc = PartialDataCube.load(dcname, basename)
    dc.cuboids.map { c => (c.n_bits, c.numBytes) }.groupBy(_._1).mapValues { cs =>
      val count = cs.length
      val sum = cs.map(_._2).sum * math.pow(10, -9) //in GB
      val avg = sum * 1000 / count //in MB
      (count, sum, avg)
    }
  }

  /** makes a sz-element ArrayBuffer and initializes each i-th field with
      init_v(i).
   */
  def mkAB[T](sz: Int, init_v: Int => T) = {
    val ab = collection.mutable.ArrayBuffer[T]()
    ab ++= (0 to sz - 1).map(init_v(_))
    ab
  }

  // Intersection for two sorted lists a and b
  def intersect(a: List[Int], b: List[Int]): List[Int] = {
    var x = a
    var y = b
    var result = List[Int]()
    while (x != Nil && y != Nil) {
      if (x.head > y.head)
        y = y.tail
      else if (x.head < y.head)
        x = x.tail
      else {
        result = x.head :: result
        x = x.tail
        y = y.tail
      }
    }
    result.reverse
  }

  /**
   * Intersection for two sorted lists a and b, optimized for hashmap uses of unique Int representation
   * !! MAX LENGTH OF a = 32 !! Otherwise, might? cause overflow
   * @param a First list (query in case of use in Prepare)
   * @param b Second list (projection "")
   * @return (intersection as list, intersection as unique Int)
   */
  def intersect_intval3(a: List[Int], b: List[Int]): (List[Int], Int) = {
    var x = a
    var y = b
    var inter_int = 0
    var index = 1
    var result = List[Int]()
    while (x != Nil && y != Nil) {
      if (x.head > y.head)
        y = y.tail
      else if (x.head < y.head) {
        index = index << 1
        x = x.tail
      }
      else {
        result = x.head :: result
        inter_int += index
        x = x.tail
        index = index << 1
        y = y.tail
      }
    }
    (result.reverse, inter_int)
  }

  def complement[T](univ: Seq[T], s: Seq[T]) =
    univ.filter(x => !s.contains(x))

  def subsumes[T](s1: Seq[T], s2: Seq[T]) = {
    //s2.forall(x => s1.contains(x))
    (s1.size >= s2.size) &&
      s2.toSet.subsetOf(s1.toSet)
    //    (s1.union(s2).toSet.size == s1.toSet.size)
  }

  def filterIndexes[T](s: Seq[T], is: Seq[Int]) =
    s.zipWithIndex.filter { case (_, i) => is.contains(i) }.map(_._1)

  /** careful -- this method may not terminate in case nondeterministic
       function <sample> cannot supply n distinct values, and it will be
       SLOW if we are asking for closeto the maximum of distinct values.
   */
  def collect_n[T](n: Int, sample: () => T): List[T] = {
    val s = collection.mutable.Set[T]()
    while (s.size < n) s.add(sample())
    s.toList
  }

  def rnd_choose(n: Int, k: Int): List[Int] =
    collect_n[Int](k, () => scala.util.Random.nextInt(n)).sorted

  /** compares two lists by the first element on which they disagree.
      If one of the lists is a prefix of the other, the function says they
      are equal! (Meant for lists of equal length.)

      Example:
      {{{
      lists_lt(List(1,5,2,6,3,6), List(1,5,1,9,9,9)) =>  <
      }}}
   */
  def lists_lt(l1: Seq[Int], l2: Seq[Int]) = {
    val i = l1.zip(l2).indexWhere((x: (Int, Int)) => x._1 != x._2)
    l1(i) < l2(i)
  }
}



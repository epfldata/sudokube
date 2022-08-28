//package ch.epfl.data.sudokube
package util


object BitUtils {
  /**
   * @param perm a permutation of (0 to bits - 1).
   *             0 is the least significant bit.
   * @example {{{
   *scala> (0 to 15).map(permute_bits(3, Array(1,2,0))(_))
   *res0: scala.collection.immutable.IndexedSeq[Int] =
   *Vector(0, 2, 4, 6, 1, 3, 5, 7, 0, 2, 4, 6, 1, 3, 5, 7)
   * }}}
   * In this example, the permutation (1,2,0) maps 0->1, 1->2, 2->0.
   */
  def permute_bits(bits: Int, perm: Array[Int]) = (i: BigInt) => {
    var result = 0;
    val bbi = BigBinary(i)
    for (j <- 0 to bits - 1) if (bbi(j) == 1) result |= 1 << perm(j)
    result
  }

  def bitToBytes(n_bits: Int) = (n_bits + 8) >> 3 //TODO: Change to +7

  /**  Returns a function for projecting a BigBinary b using a sequence of bit positions we keep (and dropping others)
   * @param bitpos indicates positions of bits that are to be kept
   * @example {{{
    scala> def f = mk_project_bitpos_f(Vector(0, 2))
    f: BigBinary => BigBinary
    scala> (0 to 9).map(x => f(BigBinary(x)))
    res1: scala.collection.immutable.IndexedSeq[BigBinary] =
    Vector(0, 1, 0, 1, 10, 11, 10, 11, 0, 1)
    }}}
   */
  def mk_project_bitpos_f(bitpos: IndexedSeq[Int]): BigBinary => BigBinary = {
    (i: BigBinary) => {
      val ibits = i.toSeq.toIndexedSeq
      val projection = bitpos.filter(_ < ibits.length).zipWithIndex.map { case (b, idx) => (ibits(b): BigInt) << idx }.sum
      BigBinary(projection)
    }
  }

  /**
      @param selection order of elements does not matter, no permutation
             is specified here. Note: selection need not be a subset of
             universe.
      @example {{{
          scala> mk_list_maskpos(Vector("B", "A", "C"), Set("C", "A"))
          res1: IndexedSeq[Int] = Vector(1, 2)

          scala> mk_list_maskpos(Vector("A", "B", "C"), Set("C", "A"))
          res2: Seq[Int] = Vector(0, 2)

          scala> mk_list_maskpos(Vector("A", "B", "C"), Set("A", "C"))
          res3: Seq[Int] = Vector(0, 2)
      }}}
   */
  def mk_list_bitpos[T](universe: IndexedSeq[T], selection: Set[T]): IndexedSeq[Int] = {
    universe.indices.filter(i => selection.contains(universe(i)))
  }


  /** @example {{{
          scala> Bits.group_values(List(0), 0 to 2)
          res2: IndexedSeq[IndexedSeq[Int]] =
            Vector(Vector(0, 2, 4, 6), Vector(1, 3, 5, 7))

          scala> Bits.group_values(List(0,1), 0 to 2)
          res3: IndexedSeq[IndexedSeq[Int]] =
            Vector(Vector(0, 4), Vector(1, 5), Vector(2, 6), Vector(3, 7))
          }}}

          TODO: currently creates IndexedSeq[IndexedSeq[Int]], rather than
          of BigBinary
   */
  def group_values(bits: Seq[Int], universe: Seq[Int]): Seq[Seq[BigBinary]] = {
    assert(bits.toSet.subsetOf(universe.toSet))

    val bits2 = Util.complement(universe, bits)
    val n_vals1 = 1 << bits.length
    val n_vals2 = 1 << (universe.length - bits.length)

    for (i <- 0 to n_vals1 - 1) yield {
      val ii = BigBinary(i).pup(bits)
      for (j <- 0 to n_vals2 - 1) yield (ii + BigBinary(j).pup(bits2))
    }
  }

  /**
   *  Special case of group values where universe is 0 until universeLength and bits is encoded using Int
   *  */
  def group_values_Int(bits: Int, universeLength: Int): Seq[Seq[BigBinary]] = {
    assert(universeLength < 31)
    val universeInt = (1 << universeLength) - 1

    val bits2 = universeInt - bits //complement
    val bitsLength = sizeOfSet(bits)
    val n_vals1 = 1 << bitsLength
    val n_vals2 = 1 << (universeLength - bitsLength)

    for (i <- 0 to n_vals1 - 1) yield {
      val ii =  unprojectIntWithInt(i, bits)
      for (j <- 0 to n_vals2 - 1) yield BigBinary(ii + unprojectIntWithInt(j, bits2))
    }
  }

  /**
   * Returns the maximum value in each group as defined above.
   */
  def max_group_values_Int(bits: Int, universeLength: Int): Seq[Int] = {
    assert(universeLength < 31)
    val universeInt = (1 << universeLength) - 1

    val bits2 = universeInt - bits //complement
    val bitsLength = sizeOfSet(bits)
    val n_vals1 = 1 << bitsLength
    val n_vals2 = 1 << (universeLength - bitsLength)
    val jj = unprojectIntWithInt(n_vals2 - 1, bits2)
    //all ones

    for (i <- 0 to n_vals1 - 1) yield {
      val ii = unprojectIntWithInt(i, bits)
      (ii + jj)
    }
  }

  /**
   * Converts an integer to a mask containing 0 and 1
   *
   * @param l Length of the mask (number of bits)
   * @param i Integer whose binary value becomes represents the mask
   * @return Mask in the form of vector
   * @example {{{
   *    scala> intToMask(5, 3)
   *    val res0: IndexedSeq[Int] = Vector(1, 1, 0, 0, 0)
   *
   *    scala> intToMask(5, 30)
   *    val res1: IndexedSeq[Int] = Vector(0, 1, 1, 1, 1)
   * }}}
   * */
  def intToMask(l: Int, i: Int): IndexedSeq[Int] = {
    assert(l < 31)
    val a = Array.fill(l)(0)
    var l2 = 0
    var i2 = i
    while (l2 < l) {
      a(l2) = i2 & 0x1
      i2 >>= 1
      l2 += 1
    }
    a.toVector
  }

  /**
   * Converts a mask containing 0 and 1 to an integer with the same binary encoding
   *
   * @param m Mask to be converted to Int
   * @return i (Big)Integer whose binary value becomes represents the mask
   * @example {{{
   *      scala> maskToInt( List(0, 1, 1))
   *      val res0: Int = 6
   *
   *      scala> maskToInt( List(1,1,1,0,1))
   *      val res1: Int = 23
   * }}}
   * */
  def maskToInt(m: Seq[Int]): Int = {
    assert(m.length < 31)
    m.foldLeft((0, 0)) { case ((sum, pow), cur) => (sum + (cur << pow), pow + 1) }._1
  }

  /**
   * Converts sequence of ints representing bit positions to a (Big)Int.
   * TODO: Use BigInt
   * */
  def SetToInt(bits: Seq[Int]) = {
    bits.foldLeft(0) { (acc, cur) => acc + (1 << cur)
    }
  }

  /**
   * Extract bit positions that are set in the binary representation of i
   */
  def IntToSet(i: Int) = {
    var i2 = i
    var result = List[Int]()
    var pos = 0
    while (i2 > 0) {
      if ((i2 & 0x1) == 1)
        result = pos :: result
      pos += 1
      i2 = i2 >> 1
    }
    result
  }
  /** Returns the hamming weight of a set encoded as Int */
  def sizeOfSet(i: Int) = {
    var hw = 0
    var i2 = i
    while(i2 > 0) {
      if ((i2 & 1) != 0)
        hw += 1
      i2 >>= 1
    }
    hw
  }
  //Returns the hamming weight, positions of 0 and positions of i from bits 0 --- maxb-1
  def hwZeroOne(i: Int, maxb: Int) = {
    var i2 = i
    var result1 = List[Int]()
    var result0 = List[Int]()
    var pos = 0
    var hw = 0
    var maxb2 = maxb
    while (maxb2 > 0) {
      if ((i2 & 0x1) == 1) {
        result1 = pos :: result1
        hw += 1
      } else
        result0 = pos :: result0
      pos += 1
      i2 = i2 >> 1
      maxb2 -= 1
    }
    (hw, result0, result1)
  }

  def projectIntWithInt(i: Int, idxes: Int): Int = {
    var idx2 = idxes
    var result = 0
    var i2 = i
    var pos = 0
    while (i2 > 0 && idx2 > 0) {

      if ((idx2 & 0x1 ) == 1) {
        result += (i2 & 0x1) << pos
        pos += 1
      }
      i2 = i2 >> 1
      idx2 = idx2 >> 1
    }
    result
  }

  //un-projects the number i to bits represented by idxes. Choosing idxes = 2^i - 1 should have no effect.
  def unprojectIntWithInt(i: Int, idxes: Int) = {
    var i2 = i
    var idx2 = idxes
    var shift = 0
    var result = 0
    while (idx2 > 0) {
      if ((idx2 & 0x1) == 1) {
        result += (i2 & 0x1) << shift
        i2 = i2 >> 1
      }
      shift += 1
      idx2 = idx2 >> 1
    }
    result
  }
}



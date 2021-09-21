//package ch.epfl.data.sudokube
package util


object Bits {
  /**
    @param perm a permutation of (0 to bits - 1).
    0 is the least significant bit.

    {{{
    scala> (0 to 15).map(permute_bits(3, Array(1,2,0))(_))
    res0: scala.collection.immutable.IndexedSeq[Int] =
      Vector(0, 2, 4, 6, 1, 3, 5, 7, 0, 2, 4, 6, 1, 3, 5, 7)
    }}}
    In this example, the permutation (1,2,0) maps 0->1, 1->2, 2->0.
  */
  def permute_bits(bits: Int, perm: Array[Int]) = (i: BigInt) => {
    var result = 0;
    val bbi = BigBinary(i)
    for(j <- 0 to bits - 1) if(bbi(j) == 1) result |= 1 << perm(j)
    result
  }

  /** Returns a function for projecting a BigBinary b  using a mask (i.e. keeping those bits
      of b that are 1 in mask, and dropping the others), considering the mx least significant
      bits.
      For example,
      {{{
      val abcde = 22 // five arbitrary bits, e.g. 10110

      assert(abcde < 32)
      val bce   = (abcde >> 2) % 4 << 1 + (abcde % 2)
      val mask = BigBinary(13)   // 13 is 1101
      val mx    = 4    // mx needs to be at least 4 since the mask is 4 bits
      def f = Bits.mk_project_f(mask, mx) 
      assert(f(BigBinary(abcde)) == BigBinary(bce))
      }}}
      So, informally, Bits.mk_project_f(1101, 4)(abcde) = bce, where a,b,c,d,e are bits.
  */
  def mk_project_f(mask: BigBinary, mx: Int) : BigBinary => BigBinary = {
    (i: BigBinary) => {
      var k = 0
      BigBinary((for(b <- 0 to mx - 1 if(mask(b) == 1)) yield {
        val r = (i(b): BigInt) << k
        k += 1
        r
      }).sum)
    }
  }

  /** @param mask is a sequence of zeros and ones; one if the bit at this
             position in i is to be kept.

      Example:
      {{{
      scala> def f = mk_project_f(List(1, 0, 1, 0))
      f: BigBinary => BigBinary
      scala> (0 to 9).map(x => f(BigBinary(x)))
      res1: scala.collection.immutable.IndexedSeq[BigBinary] =
        Vector(0, 1, 0, 1, 10, 11, 10, 11, 0, 1)
      }}}
  */
  def mk_project_f(mask: Seq[Int]): BigBinary => BigBinary = {
    (i: BigBinary) => {
      BigBinary(i.toSeq.zip(mask).filter(_._2 == 1
           ).zipWithIndex.map{
        case ((b, _), idx) => (b: BigInt) << idx
      }.sum)
    }
  }

  /**
      @param selection order of elements does not matter, no permutation
             is specified here. Note: selection need not be a subset of
             universe.
      {{{
      scala> mk_list_mask(List("B", "A", "C"), Set("C", "A"))
      res1: Seq[Int] = List(0, 1, 1)

      scala> mk_list_mask(List("A", "B", "C"), Set("C", "A"))
      res2: Seq[Int] = List(1, 0, 1)

      scala> mk_list_mask(List("A", "B", "C"), Set("A", "C"))
      res3: Seq[Int] = List(1, 0, 1)
      }}}
  */
  def mk_list_mask[T](universe: Seq[T], selection: Set[T]) : Seq[Int] = {
    universe.map(x => if(selection.contains(x)) 1 else 0)
  }

  /** {{{
      scala> val m = mk_bits_mask(List("B", "A", "C"), Set("B", "C"))
      m: BigInt = 5
      scala> def f = mk_project_f(BigBinary(m), 4)
      f: BigBinary => BigBinary
      scala> (0 to 9).map(x => f(BigBinary(x)))
      res0: scala.collection.immutable.IndexedSeq[util.BigBinary] =
        Vector(0, 1, 0, 1, 10, 11, 10, 11, 0, 1)
      }}}
  */
  def mk_bits_mask[T](universe: Seq[T], selection: Set[T]) : BigInt = {
    mk_list_mask(universe, selection).zipWithIndex.map{
      case (b, i) => (b: BigInt) << i
    }.sum
  }

  /** {{{
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
  def group_values(bits: Seq[Int], universe: Seq[Int]) : Seq[Seq[BigBinary]] = {
    assert(bits.toSet.subsetOf(universe.toSet))

    val bits2 = Util.complement(universe, bits)
    val n_vals1 = 1 << bits.length
    val n_vals2 = 1 << (universe.length - bits.length)

    for(i <- 0 to n_vals1 - 1) yield {
      val ii = BigBinary(i).pup(bits)
      for(j <- 0 to n_vals2 - 1) yield (ii + BigBinary(j).pup(bits2))
    }
  }

  /**
   * Returns the maximum value in each group as defined above.
   */
  def max_group_values(bits: Seq[Int], universe: Seq[Int]): Seq[Int] = {
    assert(bits.toSet.subsetOf(universe.toSet))
    val bits2 = Util.complement(universe, bits)
    val n_vals1 = 1 << bits.length
    val n_vals2 = 1 << (universe.length - bits.length)
    val jj = BigBinary(n_vals2 -1).pup(bits2)
    //all ones

    for(i <- 0 to n_vals1 - 1) yield {
      val ii = BigBinary(i).pup(bits)
      (ii + jj).toInt
    }
  }
}



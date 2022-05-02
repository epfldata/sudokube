//package ch.epfl.data.sudokube
package combinatorics
import util._
import scala.collection.Map


/** Tools for calculating the degrees of freedom of a sudokube LP instance
    (the number or variables minus the number of linearly independent
    equations).
*/
object DF {
  import Combinatorics.incl_excl

  /** build all the n-bit integers in which the free_bits
      (0 being the least significant bit)
      take values from {0,1} and the remaining bits are one.

      Example: expand *1*11 into {01011, 01111, 11011, 11111}
      {{{
      scala> mk_det(5, List(2,4))
      res1: IndexedSeq[(Int, BigBinary)] = Vector((0,1011), (1,1111), (2,11011), (3,11111))
      }}}

      We assume n can be large but free_bits.length is small.
  */
  def mk_det(n: Int, free_bits: Seq[Int]) : IndexedSeq[(Int, BigBinary)]= {
    assert(free_bits.length <= n)
    val ones  = BigBinary((Big.pow2(n - free_bits.length)) - 1)
    val bits2 = Util.complement(0 to n - 1, free_bits)
    val x     = ones.pup(bits2)
    val sz    = 1 << free_bits.length // there are not too many free_bits
                                      // to compute pow2 in this way.
    for(i <- 0 to sz - 1) yield (i, BigBinary(i).pup(free_bits) + x)
  }


  /** compute degrees of freedom for <n> query bits and a set of cuboids l
      that have already been projected down to the query bits (i.e. the sets
      in l contain only query bits and the query bits have been normalized
      to 0 .. n-1).

      Examples:
      {{{
      compute_df(3, List(List(0,1,2))) == 0
      compute_df(3, List(List(0,1), List(0,2), List(1,2))) == 1
      compute_df(3, List(List(0,1), List(0,2))) == 2
      }}}
      In the first case, the cuboid covers all the query bits, so df=0.
  */
  def compute_df(n: Int, l: List[List[Int]]) : BigInt = {
    // n-1 in binary
    val ones = BigBinary(Big.pow2(n) - 1)

    // marks free bits with 1; converts sets to binary representation
    // Example: for l = List(List(0,1), List(0,2)),  l2 = List(11, 101)
    val l2 : List[BigBinary] = l.map(ones.pup(_))

    /* 2^(number of overlapping bits)
       Example:
       cost(List(011, 101)) = 2^((111 & 011 & 101 = 001).hamming_weight) = 2^1
    */
    def cost(l: List[BigBinary]) : BigInt =
      Big.pow2(l.foldLeft(ones)(_&_).hamming_weight)

    // incl_excl[BigBinary](List(11, 101), cost _) = 6
    Big.pow2(n) - incl_excl[BigBinary](l2, cost _)
  }

  /** computes the same result as compute_df, but implemented as in Solver.
      {{{
      assert(DF.compute_df(n, l) == DF.compute_df0(n, l))
      }}}

      Note: compute_df avoids enumerating the equations and is faster.
  */
  def compute_df0(n: Int, l: Seq[Seq[Int]]) : BigInt = {
    // Enumerate the equations as collections of variables.
    val eqs = l.map(Bits.group_values(_, 0 to (n - 1)
                 ).map(x => x.map(_.toInt))).flatten

    /* We can compute the determined vars as the set of vars of maximal
       (or minimal) index of each equation. This is due to the special
       structure of the equations we construct here.
    */
    val det_vars = collection.mutable.Set[Int]()
    for(eq <- eqs) det_vars.add(eq.last)

    (1 << n) - det_vars.size
  }

  /** Density function of the degrees of freedom.
      Computes the distribution of degrees of freedom of the sets of
      of cardinality <l> of <k>-size subsets of a set of size <n>.

      For example, for n=5, k=3, l=2,
      {{{
      df_df(5,3,2) == Map(20 -> 30, 18 -> 15)
      }}}
      because there are comb(comb(n, k).toInt, l) = 45 two-element sets
      of 3-element subsets of a 5-element set, and 30 of them have 20
      degrees of freedom and the remaining 15 have 18 degrees of freedom.
  */
  def df_df(n: Int, k: Int, l: Int) : Map[BigInt, Int] = {
    assert(k <= n)
    val p : List[List[Int]] = Combinatorics.mk_comb(n, k)

    val schemas = Combinatorics.mk_comb(p.length, l).map(s => s.map(p(_)))

    def histogram(l: List[BigInt]) = l.groupBy(x => x).mapValues(_.length)

    histogram(schemas.map(schema => compute_df(n, schema)))
  }

  /** returns the same value as df_df(n, k, comb(n, k)).
      I can prove the correctness of this.

  */
  def hmax(n: Int, k: Int) : Int = {
    assert(n >= k)
    if(n == k) 0
    else if(k == 0) (1 << n) - 1
    else hmax(n-1, k) + hmax(n-1, k-1)
  }

  /** same as hmax. */
  def hmax2(n: Int, k: Int) =
    (for (l <- (k+1) to n) yield Combinatorics.comb(n,l)).sum
} // end object DF


/*
I renamed h to df_df.
def h = df_df

h(n, n-1, np) = 2^(n - np)
h(6,5,0) == Map(64 -> 1)
h(6,5,1) == Map(32 -> 6)
h(6,5,2) == Map(16 -> 15)
h(6,5,3) == Map(8 -> 20)
h(6,5,4) == Map(4 -> 15)
h(6,5,5) == Map(2 -> 6)
h(6,5,6) == Map(1 -> 1)

h(n, p, 1) = 2^p * (2^(n - p) - 1)
h(6, 5, 1) = 32
h(6, 4, 1) = 16 * 3 = 48
h(6, 3, 1) = 56
h(6, 2, 1) = 60
h(6, 1, 1) = 62
h(6, 0, 1) = 63


h(n, n-d, (n choose n-d)) = Sum_{i=2,4,6..} (n+1 choose n-d+i)

h(6, 5, 6) = 1
h(6, 4, 15) = 7
h(6, 3, 20) = 22
h(6, 2, 15) = 42
h(6, 1, 6) = 57
h(6, 0, 1) = 63

h(5, 4, 5) = 1
h(5, 3, (5 3)) = 6
h(5, 2, (5 2)) = 16
h(5, 1, (5 1)) = 26
h(5, 0, 1) = 31

h(4,3,4) = 1
h(4,2,6) = 5
h(4,1,4) = 11
h(4,0,1) = 15

h(3,0,comb(3,0)) == 7
h(4,1,comb(4,1)) == 11
h(5,2,comb(5,2)) == 16
h(6,3,comb(6,3)) == 22

h(4,0,comb(4,0)) == 15
h(5,1,comb(5,1)) == 26
h(6,2,comb(6,2)) == 42


h(n, n-2, comb(n, n-2) - 1) = h(n, n-2, comb(n, n-2)) + 1    -- conjecture

h(2,0,0) = Map(4 -> 1) 
h(2,0,1) = Map(3 -> 1)

h(3,1,0) = Map(8 -> 1) 
h(3,1,1) = Map(6 -> 3) 
h(3,1,2) = Map(5 -> 3) 
h(3,1,3) = Map(4 -> 1)

h(4,2,0) = Map(16 -> 1) 
h(4,2,1) = Map(12 -> 6) 
h(4,2,2) = Map(10 -> 12, 9 -> 3) -- 3 pairs without overlap, 12 with overlap
h(4,2,3) = Map(8 -> 16, 9 -> 4) 
h(4,2,4) = Map(7 -> 15) 
h(4,2,5) = Map(6 -> 6) 
h(4,2,6) = Map(5 -> 1)

h(5,3,0) = Map(32 -> 1)
h(5,3,1) = Map(24 -> 10) 
h(5,3,2) = Map(20 -> 30, 18 -> 15)
h(5,3,3) = Map(16 -> 70, 18 -> 20, 15 -> 30)
h(5,3,4) = Map(17 -> 5, 14 -> 135, 13 -> 60, 12 -> 10)
h(5,3,5) = Map(11 -> 72, 13 -> 30, 12 -> 150)
h(5,3,6) = Map(11 -> 70, 10 -> 140)
h(5,3,7) = Map(10 -> 10, 9 -> 110)
h(5,3,8) = Map(8 -> 45)
h(5,3,9) = Map(7 -> 10)
h(5,3,10) = Map(6 -> 1)

h(6,4,0) = Map(64 -> 1) 
h(6,4,1) = Map(48 -> 15) 
h(6,4,2) = Map(40 -> 60, 36 -> 45) 
h(6,4,3) = Map(32 -> 200, 27 -> 15, 36 -> 60, 30 -> 180) 
h(6,4,4) = Map(24 -> 240, 25 -> 90, 28 -> 585, 34 -> 30, 27 -> 60, 26 -> 360) 
h(6,4,5) = Map(24 -> 900, 20 -> 60, 21 -> 585, 33 -> 6, 22 -> 792, 26 -> 300, 23 -> 360) 
h(6,4,6) = Map(25 -> 60, 20 -> 1665, 21 -> 180, 22 -> 960, 18 -> 510, 16 -> 10, 19 -> 1620) 
h(6,4,7) = Map(20 -> 360, 21 -> 180, 17 -> 2520, 18 -> 2460, 16 -> 720, 19 -> 90, 15 -> 105) 
h(6,4,8) = Map(14 -> 660, 17 -> 420, 16 -> 2880, 19 -> 135, 15 -> 2340) 
h(6,4,9) = Map(14 -> 2520, 13 -> 1560, 18 -> 15, 15 -> 910) 
h(6,4,10) = Map(14 -> 90, 13 -> 1140, 12 -> 1773) 
h(6,4,11) = Map(11 -> 1125, 12 -> 240) 
h(6,4,12) = Map(11 -> 20, 10 -> 435) 
h(6,4,13) = Map(9 -> 105) 
h(6,4,14) = Map(8 -> 15) 
h(6,4,15) = Map(7 -> 1) 

h(7,5,comb(7,5)) = Map(8 -> 1) 
h(7,5,comb(7,5)-1) = Map(9 -> 21) 
h(7,5,comb(7,5)-2) = Map(10 -> 210) 

h(8,6,comb(8,6)) = Map(9 -> 1) 
h(8,6,comb(8,6) - 1) = Map(10 -> 28)

*/



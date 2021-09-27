//package ch.epfl.data.sudokube
package combinatorics
import util._


object Big {
  val one = (1: BigInt)
  def pow2(n: Int) = one << n

  /** m to the k-th power
      @param k may be negative
  */
  def pow(m: BigInt, k: Int) : BigDecimal = {
    if(k >= 0) BigDecimal((1 to k).map(_ => m).product)
    else BigDecimal(1) / BigDecimal((1 to -k).map(_ => m).product)
  }

  /** log10(pow(1000, 5)) == 15 */
  def log10(n: BigInt) = n.toString.length - 1

  /** divide the product of zl by the product of nn.
      zl and nn must have the same number of elements.
      TODO: otherwise pad
  */
  def ratio(zl: Seq[BigInt], nn: Seq[BigInt]) : BigDecimal = {

    assert(zl.length == nn.length)
    
    zl.sorted.zip(nn.sorted).map(
      x => BigDecimal(x._1) / BigDecimal(x._2)).product
  }
}


object Combinatorics {
  def factorial(n : Int): BigInt = {
    if (n == 0) 1
    else (Big.one to n).product
  }

  /** count combinations / (n choose k) == n! / (k! * (n-k)!).
      How many distinct ways are there of
      picking a k-element subset of an n-element set?
      If, n < k, comb(n, k) == 0.
  */
  def comb(n: Int, k: Int) : BigInt = {
    if(n < k) BigInt(0)
    else (BigInt(n - k + 1) to n).product / factorial(k)
  }

  /// the naive implementation of n choose k.
  def comb_naive(n: Int, k: Int) : BigInt = {
    if(n < k) BigInt(0)
    else factorial(n) / (factorial(k) * factorial(n - k))
  }

  /// print Pascal's triangle to screen.
  def pascals_triangle(n_max: Int) = {
    for(n <- 0 to n_max) {
      for(k <- 0 to n) {
        print(comb(n, k) + " ")
      }
      println
    }
  }


  /** "labeled combinations".
      Given a set of n elements of which m have a special label,
      in how many ways can we pick k elements such that l of them have the
      label?
      This is always true:
      {{{
      def tstlc(n: Int, m: Int, k: Int) = {
        (0 to k).map(l => lcomb(n, m, k, l)).sum == comb(n, k)
      }
      }}}
  */
  def lcomb(n: Int, m: Int, k: Int, l: Int) : BigInt = {
    assert(m <= n)
    if(k < l) 0
    else      comb(m, l) * comb(n - m, k - l)
  }

  /** for fixed n and m, shows lcomb(n, m, k, l) with l, k plotted on the
      x and y axes, respectively. Shows nicely how lcomb is a product of
      combs. Zeroes are not displayed.
      {{{
      scala> lcomb_diamond(10, 4)
           1 
           6      4 
          15     24      6 
          20     60     36      4 
          15     80     90     24      1 
           6     60    120     60      6 
           1     24     90     80     15 
                  4     36     60     20 
                         6     24     15 
                                4      6 
                                       1 
      }}}
  */
  def lcomb_diamond(n: Int, m: Int) = {
    val k_max = n
    for(k <- 0 to k_max) {
      for(l <- 0 to math.min(k, m)) {
        val x = lcomb(n, m, k, l)
        if(x == 0) print("       ")
        else print("%6d ".format(x))
      }
      println
    }
  }

  /** Given a set of n elements of which m have a special label,
      in how many ways can we pick k elements such that *at least*
      l of them have the label?

    Sums up the values in the row of the lcomb_diamond to the right of a field.
    Example:
    lcomb_sup(10, 4, 3, 1) = lcomb(10, 4, 3, 1) + lcomb(10, 4, 3, 2) +
       lcomb(10, 4, 3, 3) = 60 + 36 + 4 = 100
  */
  def lcomb_sup(n: Int, m: Int, k: Int, l: Int) =
    (l to m).map(l0 => lcomb(n, m, k, l0)).sum

  /** Given a set of n elements of which m have a special label,
      in how many ways can we pick k elements such that *at most*
      l of them have the label?

    Sums up the values in the row of the lcomb_diamond to the left of a field.
    Example:
    lcomb_inf(10, 4, 3, 1) = lcomb(10, 4, 3, 1) + lcomb(10, 4, 3, 0) =
       60 + 20 = 80
  */
  def lcomb_inf(n: Int, m: Int, k: Int, l: Int) =
    (0 to l).map(l0 => lcomb(n, m, k, l0)).sum


  /** size of a complete data cube with m elements per dimension and
      d dimensions.

      Always true:
      {{{
      complete_cube_storage(m, d) == Big.pow(m+1, d)
      }}}
      This is the faster way to implement it.
      The implementation below shows the definition.
  */
  def complete_cube_storage(m: Int, d: Int) =
    (0 to d).map(k => comb(d, k) * Big.pow(m, k).toBigInt).sum


  /*
  /// The naive implementation of f0; this is slow.
  def f1(n: Int, m: Int, k: Int, l: Int): Double = {
    lcomb(n, m, k, l).toDouble / comb(n, k).toDouble
  }
  */

  /** When we randomly choose k elements from an
      n-element set of which m elements have a special label,
      the probability that exactly l of the chosen elements have that label.

      Example:
      {{{
      f0(10, 4, 2, 1) == 24.0 / (15 + 24 + 6) = 0.533333
      }}}

      Per definition, this is
      {{{
      lcomb(n, m, k, l).toDouble / comb(n, k).toDouble
      }}}
      but this is slow to compute. We optimize it as
      {{{
      fac(m) * fac(n-m) * fac(k) * fac(n-k) /
      (fac(l) * fac(m-l) * fac(n) * fac(k-l) * fac(n-m-k+l))
      }}}
      and further.
      Big.ratio divides two products given as lists of values to be multiplied.
  */
  def f0(n: BigInt, m: BigInt, k: BigInt, l: BigInt): BigDecimal = {
    assert(n >= k)
    print(".")

    if((m < l) || (n-m < k-l)) 0.0
    else {
      val (a, b) = if (m <= k) (m, k) else (k, m)

      val numerator = ((m-l+Big.one) to m) ++ ((k-l+Big.one) to k) ++
                      ((n-m-k+l+Big.one) to (n-b))
      val denominator = (Big.one to l) ++ ((n-a+Big.one) to n)

      assert(numerator.length == denominator.length) // in each case, a+l values
      
      Big.ratio(numerator, denominator)
    }
  }


  /** The expected number of cuboids with a sufficient number of relevant
      dimensions.
      There is a set S of size n of which m elements have a special label.
      If we pick a random k0-element set of k-element subsets of S,
      in expectation, how many of them contain at least l elements with the
      special label?
      Example
      {{{
      // comb(3,2) == 3  and lcomb(3,2,2,2) == 1,
      // so there are three sets to pick from and only one that is of interest
      exp_relevant(3,2,2,2,0) == 0
      exp_relevant(3,2,2,2,1) == 1.0/3
      exp_relevant(3,2,2,2,2) == 2.0/3
      exp_relevant(3,2,2,2,3) == 1

      lcomb(3,2,2,1) == 2 // all of the three sets have at least one
      lcomb(3,2,2,2) == 1 // labelled element each
      exp_relevant(3,2,2,1,0) == 0
      exp_relevant(3,2,2,1,1) == 1
      exp_relevant(3,2,2,1,2) == 2
      exp_relevant(3,2,2,1,3) == 3
      }}}

      The interpretation is that
      * n is the number of dimensions in the data cube,
      * m is the number of dimensions of the query,
      * we materialize k0 randomly picked cuboids of k dimensions,
      * how many of these cuboids contain at least l query dimensions
        (i.e. have at least l dimensions after projection to the query
         dimensions)?

      Example:
      {{{
      scala> exp_relevant(20, 8, 15, 7, 10)
      res0: BigDecimal = 3.06501547987616099071207430340557307943
      scala> DF.df_df(8, 7, 3)
      res1: Map[BigInt,Int] = Map(32 -> 56)
      }}}
      So if we materialize ten 15-dimensional projections of a 20-dimensional
      data cube, there are in expectation about three cuboids whose
      projection to an 8-dimensional query is at least 7-dimensional.
      If there are three such cuboids, and we use only these to build the LP
      instance, there are exactly 32 degrees of freedom.

      If we pick 20 such cuboids,
      {{{
      scala> exp_relevant(20, 8, 15, 8, 20)
      res2: BigDecimal = 1.02167182662538699690402476780185763528115...
      }}}
      (out of the comb(20,15) == 15504 that exist), we can expect there to
      be one cuboid that covers the query completely.
  */
  def exp_relevant(n: Int, m: Int, k: Int, l: Int, k0: Int) : BigDecimal = {
    assert(k0 <= comb(n, k))
    val n0 : BigInt = comb(n, k)
    val m0 : BigInt = lcomb_sup(n, m, k, l)
    (0 to k0).map(l0 => f0(n0, m0, k0, l0) * l0).sum
  }


  /** expected required dimensionality of the smallest
      cuboid that can provide goalsize many dimensions of the query.
  */
  def exp_cost(n: Int, rf: Double, base: Double, mindim: Int = 0,
               qsize: Int, goalsize: Int) : Int = {

    var k = 0
    var x: BigDecimal = 0
    val m = core.RandomizedMaterializationScheme(n, rf, base, mindim)

    while((x < 1) && (k <= n)) {
      val npd = m.n_proj_d(k)
      val f = exp_relevant(n, qsize, k, goalsize, npd)
      println("k=" + k + " npd=" + npd + " exp_relevant=" + f)
      x += f
      k += 1
    }
    k - 1
  }


  /** I don't remember what this is, but it obviously means p_fail.
      I assume the "fail" refers to this being too slow to be useful.
  */
  def pfail(n: Int, m: Int, k: Int, l: Int, k0: Int) : BigDecimal = {
    val n0 : BigInt = comb(n, k)
    val m0 : BigInt = lcomb_sup(n, m, k, l)
    val result = f0(n0, m0, k0, 0)
    result
  }

  /*
  val n_bits =60 
  val rf = .1
  val base = 1.2
  val qsize = 6
  frontend.experiments.minus1_adv(n_bits, rf, base, qsize, 1000)

  val m = core.RandomizedMaterializationScheme(n_bits, rf, base, 0)
  exp_cost2(n_bits, qsize, qsize, m.n_proj_d(_))
  */
  def exp_cost2(n: Int, qsize: Int, goal_size: Int, size_f: Int => Int) = {
    val memo = (0 to n).map(d0 => pfail(n, qsize, d0, goal_size, size_f(d0)))

    (0 to n).map(d0 =>
      (0 to d0 - 1).map(memo(_)).product * (1 - memo(d0)) * BigDecimal(d0)
    ).sum
  }


  /** create combinations
    {{{
    scala> mk_comb(4,2)
    res1: List[List[Int]] = List(List(0, 1), List(0, 2), List(0, 3),
    List(1, 2), List(1, 3), List(2, 3))
    }}}
  */
  def mk_comb(n: Int, k: Int) : List[List[Int]] = {
    def mk_comb1(n: Int, k: Int, from: Int) : List[List[Int]] = {
      if(k == 0) List[List[Int]](List[Int]())
      else
      (for(i <- from to n - 1) yield {
          mk_comb1(n, k - 1, i + 1).map(x => i :: x)
      }).toList.flatten
    }

    mk_comb1(n, k, 0)
  }

  def mk_comb_bi(n: Int, k: Int) : List[BigInt] = {
    def rec(num: BigInt, ones: Int, dims: Int): List[BigInt] = {
      if (ones == dims)
        List(((num + 1) << dims) - 1)
      else if (ones == 0)
        List(num << dims)
      else {
        val num2 = num << 1
        rec(num2, ones, dims - 1) ++ rec(num2 + 1, ones - 1, dims - 1)
      }
    }
    rec(0, k, n)
  }

  /** Returns combinations of elements from a given set.
      {{{
      scala> mk_comb(List("A","B","C"), 2)
      res0: List[List[String]] = List(List(A, B), List(A, C), List(B, C))
      }}}
  */
  def mk_comb[T](l: List[T], k: Int) : List[List[T]] = {
    mk_comb(l.length, k).map(x => x.map(y => l(y)))
  }

  /** inclusion-exclusion principle.
      T is a type representing a set (but not necessarily stored as a
      collection)
      @param l  a set of representations of sets
      @param intersect_cost_f returns the weight of the intersection of a
             set of such set representations
      @return the weight of the unions of the represented sets

      For an example, see DF.compute_df().
  */
  def incl_excl[T](l: List[T], intersect_cost_f: List[T] => BigInt) : BigInt = {
    (for(i <- 1 to l.length) yield {
      val w: BigInt = mk_comb(l, i).map(intersect_cost_f(_)).sum
      val s = (i%2)*2 - 1 // alternating sign: +1 -1 +1 -1 ...
      s * w
    }).sum
  }
} // end object Combinatorics



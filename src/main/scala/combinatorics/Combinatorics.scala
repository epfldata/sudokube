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
  def comb_naive(n: Int, k: Int) : BigInt = {
    if(n < k) BigInt(0)
    else factorial(n) / (factorial(k) * factorial(n - k))
  }

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

  /** {{{
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

  def lcomb_sup(n: Int, m: Int, k: Int, l: Int) =
    (l to m).map(l0 => lcomb(n, m, k, l0)).sum

  def lcomb_inf(n: Int, m: Int, k: Int, l: Int) =
    (0 to l).map(l0 => lcomb(n, m, k, l0)).sum


  /** size of a complete data cube with m elements per dimension and
      d dimensions.

      Always true:
      {{{
      complete_cube_storage(m, d) == Big.pow(m+1, d)
      }}}
  */
  def complete_cube_storage(m: Int, d: Int) =
    (0 to d).map(k => comb(d, k) * Big.pow(m, k).toBigInt).sum


  // a slower version of f0
  def f1(n: Int, m: Int, k: Int, l: Int): Double = {
    lcomb(n, m, k, l).toDouble / comb(n, k).toDouble
  }

  /** When we randomly choose k elements from an
      n-element set of which m elements have a special label,
      the probability that exactly l of the chosen elements have that label.

      lcomb(n, m, k, l) / comb(n, k)
      or
      fac(m) * fac(n-m) * fac(k) * fac(n-k) /
      (fac(l) * fac(m-l) * fac(n) * fac(k-l) * fac(n-m-k+l)) 
  */
  def f0(n: BigInt, m: BigInt, k: BigInt, l: BigInt): BigDecimal = {
    assert(n >= k)
    //println("f0(" + n + ", " + m + ", " + k + ", " + l + ")")
    print(".")

    if((m < l) || (n-m < k-l)) 0.0
    else if (m <= k) {

      val zl = ((m-l+Big.one) to m) ++ ((k-l+Big.one) to k) ++
               ((n-m-k+l+Big.one) to (n-k))
      val nenner = (Big.one to l) ++ ((n-m+Big.one) to n)

      //println("f0(" + n + ", " + m + ", " + k + ", " + l + ")  " +
      //        "case 1:  " + zl.length)
      assert(zl.length == nenner.length) // in each case, m+l values

      Big.ratio(zl, nenner)
    }
    else {
      val zl = ((m-l+Big.one) to m) ++ ((k-l+Big.one) to k) ++
               ((n-m-k+l+Big.one) to (n-m))
      val nenner = (Big.one to l) ++ ((n-k+Big.one) to n)

      //println("f0(" + n + ", " + m + ", " + k + ", " + l + ")  " +
      //        "case 2:  " + zl.length)
      assert(zl.length == nenner.length) // in each case, k+l values
      
      Big.ratio(zl, nenner)
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


  def pfail(n: Int, m: Int, k: Int, l: Int, k0: Int) : BigDecimal = {
    val n0 : BigInt = comb(n, k)
    val m0 : BigInt = lcomb_sup(n, m, k, l)
    val result = f0(n0, m0, k0, 0)
    //println("pfail(" + n + ", " + m + ", " + k + ", " + l + ", " + k0 + ") = " + result)
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
  */
  def incl_excl[T](l: List[T], intersect_cost_f: List[T] => BigInt) : BigInt = {
    (for(i <- 1 to l.length) yield {
      val w: BigInt = mk_comb(l, i).map(intersect_cost_f(_)).sum
      val s = (i%2)*2 - 1 // alternating sign: +1 -1 +1 -1 ...
      s * w
    }).sum
  }
} // end object Combinatorics



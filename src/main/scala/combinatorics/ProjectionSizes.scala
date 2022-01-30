package combinatorics
import util._

/*
object ProjectionSizes {
  val r = new scala.util.Random()

  def factorial(n: Long) : Long = if (n > 1) n * factorial(n-1) else 1
  def choose(n: Long, k: Long) = factorial(n) / factorial(k) / factorial(n-k)

  /**
    Randomly pick 2^d0 distinct fields in a d-dimensional binary cuboid and
    make them nonzero. (That is, the sparse representation of the cuboid
    has exactly 2^d0 tuples.)
    Then project the d dimensions down to a randomly picked
    d0 < d dimensions and count how many nonzero values the resulting
    d0-dimensional cuboid has (i.e., how big its sparse representation is).
  */
  def sampleSparseProjectionSize(d: Int, d0: Int) = {
    assert(d0 > 0)
    assert(d0 < d)

    val p0 = math.pow(2, d0).toInt
    val p  = math.pow(2, d ).toInt
    var s  = collection.mutable.Set[Int]()

    while(s.size < p0) { s.add(r.nextInt(p)) }

    s.map(x => x % p0).size
  }

  /** estimate the expected ratio of sparse projection size to maximum
      possible size using n samples. If there are no collisions of nonzero
      values during the projection, this returns 1.

      Example: for d=2 and d0=1, the result approaches 5/6 because
      there are 4 choose 2 = 6 possibilities to assign 2 items to 4 slots,
      4 of these 6 have projection size 2 and 2 of the 6 have projection size
      1. So 4/6 * 2/2 + 2/6 * 1/2 = 5/6.

      approxProjectionRatio(3, 2, ...) -> 55/(8 ch 4) = 55/70 = 11/14
      approxProjectionRatio(4, 2, ...) -> 7949/6/(16 ch 4) = 7949/6/1820

  */
  def approxProjectionRatio(d: Int, d0: Int, n: Int) : Double = {
    var sm: Double = 0
    for(i <- 1 to n) {
      sm = sm + (sampleSparseProjectionSize(d, d0) / math.pow(2, d0))
    }
    (sm / n)
  }


  def projectionSizeExperiment = {
    var d0 = 22
    while(d0 < 26) {
      var d = d0 + 1
      while(d <= 31) {
        val n = if(     d0 <  5) 10000000
                else if(d0 <  8) 1000000
                else if(d0 < 10) 100000
                else if(d0 < 14) 10000
                else if(d0 < 18) 1000
                else             100
        System.out.println(d + " " + d0 + " " + n + " " +
          approxProjectionRatio(d, d0, n))
        d += 1
      }
      d0 += 1
    }
  }



  /** expectedProjectionRatio(d, 1, ...)

      expectedProjectionRatio(2, 1, ...) ->   5/( 4 ch 2) =   5/6
      expectedProjectionRatio(3, 1, ...) ->  22/( 8 ch 2) =  22/28 = 11/14
      expectedProjectionRatio(4, 1, ...) ->  92/(16 ch 2) =  92/120
      expectedProjectionRatio(5, 1, ...) -> 376/(32 ch 2) = 376/496
      expectedProjectionRatio(d->\infty, 1, ...) -> 0.75

      case d = 3
      1 * 2^4 + 1/2 * 2 * comb(2^{d-1},2) = 22
  
      case d = 4
      1/2 * 2 * comb(8,2) + 1 * 2^6 = 28 + 64 = 92

      case d = 5
      120 + 2^8 = 376
  */
  def expectedProjectionRatio1(d: Int) : Double = {
    val collisions = 2 * comb(1 << (d-1), 2)
    (collisions.toDouble / 2 + (1L << (2 * (d-1)))) / comb(1 << d, 2).toDouble
  }

  /** expectedProjectionRatio(d0+1, d0, ...)
      expectedProjectionRatio(3, 2, ...) -> 55/(8 ch 4) = 55/70 = 11/14

      #holes h

      comb(2^d0, h) * comb(2^d0 - h, h) * 2^(2^d0 - 2h)

      h=1
      comb(4, 1) * comb(3,1) * 2^2 = 4 * 3 * 4 = 48

      h=0
      1 * 1 * 16

      h=2
      6 

      4/4 * 16 + 3/4 * 48 + 2/4 * 6 = 55

      expectedProjectionRatio(4, 3, ...) -> 9867/12870 = 0.7666666666.
      expectedProjectionRatio(5, 4, ...) -> 455657715/601080390 = 0.7580645161290323
  */
  def expectedProjectionRatio_d0d0p1(d0: Int) : Double = {
    val p0 = 1 << d0

    // comb(p0, h)     ... choose the holes
    // comb(p0 - h, h) ... choose the collisions
    // 2^(2^d0 - 2h)   ... assign the remaining ones
    def f(h: Int) =
      comb(p0, h) * comb(p0 - h, h) * 1 << (p0 - 2*h)

    (0 to p0 - 1).map(h => f(h).toDouble * (p0-h)/p0).sum
  }


  /** number of different projections with h holes */
  def f(p0: Int, pfactor: Int, slots: Int, h: Int) : BigInt = {
    val rest : Int = slots - h
    val at_least_one = (1 to rest - 1).map(h0 => f(p0, pfactor, rest, h0)).sum
    comb(slots, h) * (comb(rest * pfactor, p0) - at_least_one)
  }

  // call with mul:=1
  def f3(p0: Int, pfactor: Int, slots: Int, h: Int, mul: Int) : BigInt = {
    val rest : Int = slots - h
    val mul2 = (mul * comb(slots, h)).toInt

    mul2 * comb(rest * pfactor, p0) +
      (1 to rest - 1).map(h0 => f3(p0, pfactor, rest, h0, - mul2)).sum
  }

  def h(p0: Int, slots: Int, i: Int, j: Int, holes: Int) : BigInt = {
    comb(slots, i) * comb(slots - i, holes) *
    (if((i + p0 - slots + holes) % 2 == 1) 1 else -1)
  }

  def f6(p0: Int, pfactor: Int, slots: Int, holes: Int) : BigInt = {
    def a(i: Int, j: Int) = comb(i, j) * comb((i-j) * pfactor, p0)

    a(slots, holes) +
    (2 to slots).map(i =>
      (1 to i - 1).map(j =>
        h(p0, slots, i, j, holes) * a(i,j)
      ).sum
    ).sum
  }



  def f7(p0: Int, pfactor: Int, holes: Int) : BigInt = {
    def a(i: Int, j: Int) = comb(i, j) * comb((i-j) * pfactor, p0)

    a(p0, holes) +
    (2 to p0).map(i =>
      (if((i + holes) % 2 == 1) 1 else -1) *
      comb(p0, i) * comb(p0 - i, holes) *
      (1 to i - 1).map(j =>
       a(i,j)
      ).sum
    ).sum
  }


  def ff(p0: Int, pfactor: Int, holes: Int) =
  (
    f(p0, pfactor, p0, holes),
    f7(p0, pfactor, holes)
  )


    def a2(p0: Int, pfactor: Int, i: Int) =
      (1 to i - 1).map(j =>
         comb(i, j) * comb((i-j) * pfactor, p0)
      ).sum


/*
def goo(p0: Int, pfactor: Int, slots: Int, holes: Int) = {
  var l = List[(Int, Int, Int)]()  

  def f3(p0: Int, pfactor: Int, slots: Int, h: Int, mul: Int) : BigInt = {
    l = (slots, h, mul)::l
    val rest : Int = slots - h
    val mul2 = (mul * comb(slots, h)).toInt
    
    mul2 * comb(rest * pfactor, p0) +
      (1 to rest - 1).map(h0 => f3(p0, pfactor, rest, h0, - mul2)).sum
  }

  val result = f3(p0, pfactor, slots, holes, 1)

  val gr = l.groupBy { case (i,j,l) => (i,j) }.mapValues(ll => ll.map(_._3).sum) 
  (result, gr)
}


val p0 = 4
val slots = 4
val holes = 1
val (_, gr) = goo(p0,4,slots,holes)

val gr2 = gr.groupBy{ case ((i,j), v) => i}.mapValues(x => x.toList.head._2)

gr2.map { case (i, v) => (i, (v, h(p0, slots, i, 1, holes))) }
*/
  






  /*
      expectedProjectionRatio(4, 2, ...) -> 7949/6/(16 ch 4) = 7949/6/1820

  */
  def expectedProjectionRatio(d: Int, d0: Int) = {
    val p0 = 1 << d0
    val pfactor = 1 << (d-d0)

    (0 to p0 - 1).map(h => BigDecimal(f6(p0, pfactor, p0, h)) / BigDecimal(comb(1<<d, 1<<d0)) * (p0-h)/p0).sum
  }




def derang(n: Long) = fac(n) * (0L to n).map(k =>
  (if(k % 2 == 0) 1.0 else -1.0) / fac(k)).sum


def f(n: Long) = 1.0 - derang(n).toDouble/fac(n)

(1 to 20).map(f(_))

} // end ProjectionSizes

*/


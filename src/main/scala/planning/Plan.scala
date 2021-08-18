//package ch.epfl.data.sudokube
package planning
import util._


/*
case class Plan0(
  n_bits: Int,
  initial_p: List[ProjectionMetaData] = List[ProjectionMetaData]()
) {
  var l1 = List[List[(Int, BigInt)]]()
  var l2 = List[ProjectionMetaData)]()
}
*/


/** FIXME: Plan cannot handle queries that are not ranges from 0 to n_bits-1.
    {{{
    scala> val q = List(1, 12)
    q: List[Int] = List(1, 12)

    scala> combinatorics.DF.mk_det(q, q)
    res0: IndexedSeq[(Int, BigBinary)] = Vector((0,0), (1,10), (2,10000 00000000), (3,10000 00000010))
    }}}
*/
case class Plan(
  n_bits: Int,
  initial_p: List[ProjectionMetaData] = List[ProjectionMetaData]()
) {
  /* private */
  var l = List[(List[(Int, BigBinary)], ProjectionMetaData)]()

  // the points that are not free.
  private var determined_points = List[BigBinary]()
  var dominated_projections = Set[ProjectionMetaData]()
  var alive = true // used by the optimization algorithm

  def mk_copy() = {
    val c = this.copy()
    c.l = this.l
    c.determined_points = this.determined_points
    c.dominated_projections = this.dominated_projections
    c.alive = this.alive
    c
  }

  def add_step(p: ProjectionMetaData) {
    dominated_projections = dominated_projections ++ Set(p)
    val dets  = combinatorics.DF.mk_det(n_bits, p.accessible_bits)
    val dets2 = dets.filter(x => !determined_points.contains(x._2))
    if(! dets2.isEmpty) {
      l = (dets2.toList, p)::l
      determined_points = determined_points ++ dets2.map(_._2)
    }
  }

  for(p <- initial_p) add_step(p)

  def rebuildFromPrefix(exclude_last: Int) : Plan = {
    val ps = l.map(_._2).drop(exclude_last).reverse
    Plan(n_bits, ps)
  }

  /** TODO: allow for other sort orders
      currently we always read sequentially from the start
  */
  def cost() : BigInt = {
    l.map(step => {
      val read_up_to = BigBinary(step._1.last._1)
    
      val c : BigInt = step._2.read_cost(read_up_to)
      println((read_up_to, c, step._1.last._2))
      assert(c == (step._1.last._2 + BigBinary(1)).toBigInt)
      c
    }).sum
  }

  // degrees of freedom
  def df = (1 << n_bits) - l.map(_._1).flatten.length

  override def toString = {
    val projs = l.map(_._2)
    "Plan(" + projs + " df: " + df + " cost: ?)" // + cost + ")"
  }

  def explain() = {
    var det = 0
    for(step <- l.reverse) {
      val badbits = step._2.mask.length - step._2.accessible_bits.length
      val read_up_to = BigBinary(step._1.last._1)
      val size = 1 << step._2.accessible_bits.length
      val ldet = step._1.length
      val rest = (1 << n_bits) - det - ldet
 
      println("* read cube <" + step._2.accessible_bits + " + " +
        badbits + "b> up to position " +
        (read_up_to + BigBinary(1)) + " of " + size + " at cost " +
        step._2.read_cost(read_up_to) +
        " (ra cost: " + step._2.ra_read_cost * ldet +
        "). This eliminates " + ldet + " degrees of freedom and leaves " +
        rest + ".")

      det += ldet
    }
    println("Total cost: " + cost)
  }

  def dominates(other: Plan) = {
    val b = (this.cost <= other.cost) &&
            Util.subsumes(this.determined_points, other.determined_points)

    if(b) this.dominated_projections ++= other.dominated_projections

    b
  }
} // end Plan



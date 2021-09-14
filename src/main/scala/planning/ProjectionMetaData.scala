//package ch.epfl.data.sudokube
package planning
import util._
import combinatorics._


/**
    @param accessible_bits    normalized, assumes the query is 0,1,2,..
    @param accessible_bits0   with gaps, using the original bit indexes 
                              of the MaterializationScheme
    @param mask               in the mask, the least significant bit comes first
*/
case class ProjectionMetaData(
  accessible_bits: List[Int],
  accessible_bits0: List[Int],
  mask: List[Int],
  id: Int = -1 // TODO: implement everywhere
) {
  assert(accessible_bits.length == mask.filter(_ == 1).length)
  private val indexed_mask = mask.zip((0 to mask.length - 1))
  val   accessible_bit_indexes = indexed_mask.filter(_._1 == 1).map(_._2)
  val inaccessible_bit_indexes = indexed_mask.filter(_._1 == 0).map(_._2)

  val n_inaccessible_bits = mask.filter(_ == 0).length
  val cost_factor = Big.pow2(n_inaccessible_bits)

  def read_cost(upto: BigBinary) : BigInt = {
    val ones = BigBinary((Big.pow2(n_inaccessible_bits)) - 1)

    // cost of a sequential read from the start
    val read_upto = (upto.pup(   accessible_bit_indexes) +
                     ones.pup(inaccessible_bit_indexes)).toBigInt

    read_upto + 1
  }

  // cost of a random access read
  def ra_read_cost : Long = {
    val block_bits = mask.indexOf(1)
    val blocksize = 1L << block_bits
    val num_ra = 1L << (inaccessible_bit_indexes.length - block_bits)
    val seek_cost = 10 // TODO
    val search_cost = seek_cost * mask.length
    num_ra * (search_cost + blocksize)
  }

  /* Does *this* dominate *other*, i.e., is *other* redundant ?
     Note: This returns true:
     ProjectionMetaData(List(1, 2, 3), List(1, 1, 1)).dominates(
     ProjectionMetaData(List(1), List(1, 0)))
       -- this is correct, since we have already filtered with the query:
          we want all the bits the projections can offer.

     Used in MaterializationScheme.qproject()
  */
  def dominates(other: ProjectionMetaData, cheap: Int = -1) = {
    (!(this eq other)) &&
    Util.subsumes(this.accessible_bits, other.accessible_bits) &&
    //((this.cost_factor < other.cost_factor) ||
    ((this.mask.length <= other.mask.length) ||
     (this.mask.length <= cheap))
    // careful -- see the commented-out code. Was this important for
    // optimization? It appears wrong.
  }
}


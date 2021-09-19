package backend
import core.Interval


/** A payload consisting of three values, a sum, a max, and a min (as an
    interval). Note: right now this is not able to represent three
    arbitrary measures: the interval was meant to speed up the approximation
    of the sum by giving better initial bounds to the solver. But this
    idea didn't work. We could just as well will this kind of payload, it
    just causes overheads without any upside.

    Maybe we should captue the relevant structure of a Payload by a trait
    Summable or Monoid.

    TODO: This is ugly.
*/
class Payload(var sm: Double, var i: Option[Interval[Double]]) {

  /** an aggregation function for payload with the same key (a hash collision).
      Used only in the Backend package.
  */
  def merge_in(other: Payload) {
    sm += other.sm
    i   = if(i == None) other.i
          else if(other.i == None) i
          else Some(i.get.envelope(other.i.get))
  }

  override def toString = "Payload(" + sm + ", " + i + ")"

  /** This is a hack! */
  override def equals(other: Any) = (this.toString == other.toString)
}

object Payload {
  def none          = new Payload(0, None)
  def mk(v: Double) = new Payload(v, Some(Interval(Some(v), Some(v))))

  /** takes a sequence of payloads and uses merge_in to combine them.
      Used only in ScalaBackend::sRehash::dedup.
  */
  def sum(l: Seq[Payload]) : Payload = {
    if(l.isEmpty) none
    else {
      val pp = new Payload(l.head.sm, l.head.i)
      for(p <- l.tail) pp.merge_in(p)
      pp
    }
  }

  /** This is the counterpart for the encoding in the C code.
      Triples are contatenated; an array of n triples is communicated as
      a flat array of 3*n elements. The order inside the triple is
      <min, sm, max>.
  */
  def decode_fetched(a: Array[Int]): Array[Payload] = {
    (for(i <- 0 until a.length) yield {
      val intv =  None
        //if(a(i*3) <= a(i*3+2)) Some(Interval[Double](Some(a(i*3)), Some(a(i*3+2))))
        //else                   None
      new Payload(a(i).toDouble, intv)
    }).toArray
  }
}



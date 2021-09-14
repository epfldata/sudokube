//package ch.epfl.data.sudokube
package core
import backend._
import planning.ProjectionMetaData
import util._
import java.io._
import solver._

/** To create a DataCube, must either
    (1) call DataCube.build(full_cube) or
    (2) call (companion object) DataCube.load(..)
    before use.

    An instance of the DataCube class has an associated MaterializationScheme
    that holds the metadata -- the decisions on which cuboids to materialize.

    The DataCube instance itself stores the Cuboids -- the data or proxies
    to the data held in the backend.
*/
class DataCube(val m: MaterializationScheme) extends Serializable {

  /* protected */
  var cuboids = Array[Cuboid]()

  /** this is too slow. */
  def simple_build(full_cube: Cuboid) {
    cuboids = m.projections.map(bits => full_cube.rehash_to_sparse(
      Bits.mk_list_mask[Int]((0 to m.n_bits - 1), bits.toSet).toArray)).toArray
  }

  /** build each cuboid from a smallest that subsumes it -- using
      MaterializationScheme.create_build_plan().
      has to be called explicitly, otherwise no data is created.
      In the end, the full_cube is at position cuboids.last (this is consistent
      with the order in m.projections).

      @param full_cube is integrated into the data cube as is and not copied.
  */
  def build(full_cube: Cuboid) {
    assert(full_cube.n_bits == m.n_bits)

    println("Creating build plan...")


    val cores = Runtime.getRuntime.availableProcessors/2
    val par_build_plan = m.create_parallel_build_plan(cores)
    println(s"Projecting using $cores cores...")

    // puts a ref to the same object into all fields of the array.
    val ab = Util.mkAB[Cuboid](m.projections.length, _ => full_cube)
    val backend = full_cube.backend
    val pi = new ProgressIndicator(par_build_plan.map(_.length).sum)
    val full_cube_id = par_build_plan.head.head._2
    ab(full_cube_id) = full_cube

    val threadBuffer = par_build_plan.map { build_plan =>
      new Thread {
        override def run(): Unit = {
          build_plan.foreach {
            case (_, id, -1) => ()
            case (s, id, parent_id) => {
              val mask = Bits.mk_list_mask[Int](m.projections(parent_id), s).toArray
              ab(id) = ab(parent_id).rehash(mask)

              // completion status updates
              //if(ab(id).isInstanceOf[backend.SparseCuboid]) print(".") else print("#")
              pi.step
            }
          }
        }
      }
    }

    threadBuffer.foreach(_.start())
    threadBuffer.foreach(_.join())
    cuboids = ab.toArray
    println

    // statistics output
    val real_size = cuboids.map(c => c.size).sum
    println("Real size: " + real_size + " data points ("
      + real_size.toDouble/full_cube.size.toLong + "x base cube size).")
  }

  /** load the cuboids from files. Do not call this directly, but call
      the load method of the companion object instead.

      @param l  a list of (isSparseCuboid, n_bits, size) triples, one list
                entry for each cuboid to load.
  */
  protected def load(backend: Backend[Payload], l: List[(Boolean, Int, BigInt)], name_prefix: String) {
    cuboids = (l.zipWithIndex.map {
      case ((sparse, n_bits, size), id) =>
        backend.readCuboid(id, sparse, n_bits, size, name_prefix)
    }).toArray
  }

  /** write the DataCube's metadata and cuboid data to a file.

      @param filename is the filename used for the metadata.
  */
  def save(filename: String) {
    val be = cuboids(0).backend
    val file = new File("cubedata/"+filename+"/"+filename+".dc")
    if(!file.exists())
      file.getParentFile.mkdirs()
    val l : List[(Boolean, Int, BigInt)] = (cuboids.zipWithIndex.map {
      case (c, id) => {
        val sparse = c.isInstanceOf[be.SparseCuboid];
        be.writeCuboid(id, c, file.getParent); // this actually goes to the backend.
        (sparse, c.n_bits, c.size)
      }
    }).toList

    val oos = new ObjectOutputStream(new FileOutputStream(file))
    oos.writeObject(m)
    oos.writeObject(l)
    oos.close
  }


  /** This is the only place where we transfer data from C into Scala.
  */
  protected def fetch(pms: List[ProjectionMetaData]) : Array[Payload] = {
    if(cuboids.length == 0) throw new Exception("Need to build first!")

    val backend = cuboids(0).backend

    (for(pm <- pms) yield {
      val c = cuboids(pm.id).rehash_to_dense(pm.mask.toArray)
      c.asInstanceOf[backend.DenseCuboid].fetch.asInstanceOf[Array[Payload]]
    }).flatten.toArray
  }

  /** Gets rid of the Payload box. */
   def fetch2[T](pms: List[ProjectionMetaData]
  )(implicit num: Fractional[T]) : Seq[T] = {
    fetch(pms).map(p => num.fromInt(p.sm.toInt))
  }

  /** returns a solver for a given query. One needs to explicitly compute
      and extract bounds from it. (See the code of DataCube.online_agg() for
      an example.)
  */
  def solver[T](query: List[Int], max_fetch_dim: Int
  )(implicit num: Fractional[T]) : SparseSolver[T] = {

    val l      = m.prepare(query, max_fetch_dim, max_fetch_dim)
    val bounds = SolverTools.mk_all_non_neg[T](1 << query.length)

    SparseSolver[T](query.length, bounds, l.map(_.accessible_bits), fetch2(l))
  }

  /** lets us provide a callback function that is called for increasing
      bit depths of relevant projections (as provided by
      MaterializationScheme.prepare_online_agg()).

  Example:
  {{{
    import frontend.experiments.Tools._
    import core._
    import RationalTools._

    class CB {
      var bounds : Option[collection.mutable.ArrayBuffer[Interval[Rational]]] =
        None

      def callback(s: SparseSolver[Rational]) = {
        println(s.bounds)
        bounds = Some(s.bounds)
        true
      }
    }

    val dc = mkDC(10, 1, 2, 10)
    val cb = new CB
    dc.online_agg[Rational](List(0,1,2), 2, cb.callback)
    val final_result = cb.bounds.get
  }}}
  */
  def online_agg[T](
    query: List[Int],
    cheap_size: Int,
    callback: SparseSolver[T] => Boolean, // returns whether to continue
    sliceFunc: Int => Boolean = _ => true  // determines what variables to filter
  )(implicit num: Fractional[T]) {

    var l      = m.prepare_online_agg(query, cheap_size)
    val bounds = SolverTools.mk_all_non_neg[T](1 << query.length)
    val s      = SparseSolver[T](query.length, bounds, List(), List(), sliceFunc)
    var df     = s.df
    var cont   = true
    println("START ONLINE AGGREGATION")
    while((! l.isEmpty) && (df > 0) && cont) {
      println(l.head.accessible_bits)
      s.add2(List(l.head.accessible_bits), fetch2(List(l.head)))
      if(df != s.df) {  // something added
                 // TODO: we can determine this without fetching the cuboid

        s.gauss(s.det_vars)
        s.compute_bounds
        cont = callback(s)
        df = s.df
      }
      l = l.tail
    }
  }

  /** fetches the smallest subsuming cuboid and projects away columns not in
      the query. Does not involve a solver.
  */
  def naive_eval(query: List[Int]) : Array[Double] = {
    val l = m.prepare(query, m.n_bits, m.n_bits)
    fetch(l).map(p => p.sm)
  }
} // end DataCube


object DataCube {
  /** creates and loads a DataCube.
      @param filename is the name of the metadata file.
  */
  def load(filename: String, be: Backend[Payload] = CBackend.b) : DataCube = {
    val file = new File("cubedata/"+filename+"/"+filename+".dc")
    val ois = new ObjectInputStream(new FileInputStream(file))
    val m = ois.readObject.asInstanceOf[MaterializationScheme]
    val l = ois.readObject.asInstanceOf[List[(Boolean, Int, BigInt)]]
    ois.close

    val dc = new DataCube(m)
    dc.load(be, l, file.getParent)

    dc
  }
}


/*
val dc = frontend.experiments.Tools.mkDC(10, 0.5, 1.8, 100,
  backend.ScalaBackend)
dc.save("hello")
core.DataCube.load("hello", backend.ScalaBackend)
*/


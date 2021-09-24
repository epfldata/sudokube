//package ch.epfl.data.sudokube
package core
import backend._
import planning.ProjectionMetaData
import util._

import java.io._
import solver._

import scala.collection.mutable.ArrayBuffer

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

  /**
   * Builds this cube using the base_cuboid of another DataCube
   */
  def buildFrom(that: DataCube): Unit = {
    val full_cube = that.cuboids.last
    build(full_cube)
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

    val parallel = true
    val ab = Util.mkAB[Cuboid](m.projections.length, _ => full_cube)
    if(parallel) {
      val cores = Runtime.getRuntime.availableProcessors / 2
      val par_build_plan = Profiler("CreateBuildPlan") {
        m.create_parallel_build_plan(cores)
      }
      println(s"Projecting using $cores threads")

      // puts a ref to the same object into all fields of the array.
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
                val mask = Bits.mk_list_mask[Int](m.projections(parent_id), s.toSet).toArray
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
      Profiler("Projections") {
        threadBuffer.foreach(_.join())
      }
    } else {
      val build_plan = m.create_build_plan()

      println("Projecting...")

      // puts a ref to the same object into all fields of the array.
      val backend = full_cube.backend
      val pi = new ProgressIndicator(build_plan.length)

      build_plan.foreach {
        case (_, id, -1) => ab(id) = full_cube
        case (s, id, parent_id) => {
          val mask = Bits.mk_list_mask[Int](m.projections(parent_id), s).toArray
          ab(id)   = ab(parent_id).rehash(mask)

          // completion status updates
          if(ab(id).isInstanceOf[backend.SparseCuboid]) print(".") else print("#")
          pi.step
        }
      }
    }
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

  def load2(be: Backend[Payload], multicuboidLayout: List[(List[Int], List[Boolean], List[Int], List[BigInt])], parentDir: String): Unit = {
    cuboids = new Array[Cuboid](m.projections.length)
    val parallelism =  Runtime.getRuntime.availableProcessors / 2
    println(s"Reading cuboids from disk using $parallelism threads")
    val threadBuffer = multicuboidLayout.zipWithIndex.groupBy(_._2 % parallelism).values.map { mcs =>
      new Thread {
        override def run(): Unit = {
          mcs.foreach {
            case ((idList, sparseList, nbitsList, sizeList), mcid) =>
              val filename = parentDir + s"/multicube_$mcid.csuk"
              //WARNING Converting size of cuboid from BigInt to Int
              val cmap = be.readMultiCuboid(filename, idList.toArray, sparseList.toArray, nbitsList.toArray, sizeList.map(_.toInt).toArray)
              cmap.foreach { case (cid, cub) => cuboids(cid) = cub }
            case x => assert(false)
          }
        }
      }
    }
    threadBuffer.foreach(_.start())
    threadBuffer.foreach(_.join())
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

  final val threshold = 1000 * 1000 * 10

  def save2(filename: String) {
    val be = cuboids(0).backend
    val file = new File("cubedata/"+filename+"/"+filename+".dc2")
    if(!file.exists())
      file.getParentFile.mkdirs()

    def cubTotalSize(cub: Cuboid) = cub.size * (cub.n_bits/8 + 5)
     val multiCuboids =  cuboids.zipWithIndex.foldLeft((Map[Int, Cuboid]() :: Nil) -> BigInt(0) ) {
       case ((list,total), (cub, id)) => if((total + cubTotalSize(cub)) < threshold) {
         ((list.head + (id -> cub)) :: list.tail) -> (total + cubTotalSize(cub) )
       } else {
         (Map(id -> cub) :: list) -> cubTotalSize(cub)
       }
     }._1

    type MultiCuboid = Map[Int, Cuboid]
    def extractCuboidData(mc: MultiCuboid) = {
      mc.foldLeft((List[Int](), List[Boolean](), List[Int](), List[BigInt]())) {
        case ((idList, sparseList, nbitsList, sizeList), (id, cub)) =>
          (id :: idList, cub.isInstanceOf[be.SparseCuboid] :: sparseList, cub.n_bits :: nbitsList, cub.size :: sizeList)
      }
    }

    val oos = new ObjectOutputStream(new FileOutputStream(file))
    oos.writeObject(m)
    val multiCuboidLayoutData = multiCuboids.map(extractCuboidData)
    oos.writeObject(multiCuboidLayoutData)
    oos.close

    val parallelism =  Runtime.getRuntime.availableProcessors / 2
    println(s"Writing cuboids to disk using $parallelism threads")
    val threadTasks =  multiCuboids.zipWithIndex.groupBy(_._2 % parallelism).values.map{ multicuboids =>
      new Thread {
        override def run(): Unit = {
          multicuboids.map { case (mc, mcid) =>
            val filename = file.getParent + s"/multicube_$mcid.csuk"
            //layout data is written in reverse order, so we reverse it here too
            be.writeMultiCuboid(filename, mc.values.toArray.reverse)
        }
      }
    }}

    threadTasks.foreach(_.start())
    threadTasks.foreach(_.join())
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

    val l      = Profiler("SolverPrepare"){m.prepare(query, max_fetch_dim, max_fetch_dim)}
    val bounds = SolverTools.mk_all_non_neg[T](1 << query.length)

    val data = Profiler("SolverFetch"){fetch2(l)}
    Profiler("SolverConstructor"){new SliceSparseSolver[T](query.length, bounds, l.map(_.accessible_bits), data)}
  }

  /** lets us provide a callback function that is called for increasing
      bit depths of relevant projections (as provided by
      MaterializationScheme.prepare_online_agg()).

      @param cheap_size is the number of dimensions up to which cuboids
             can be prepared for the initial iteration. cheap_size refers
             to the actual dimensionality of the cuboid, not to their
             dimensionality after projection to dimensions occurring in the
             query.

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
    val s      = new SliceSparseSolver[T](query.length, bounds, List(), List(), sliceFunc)
    var df     = s.df
    var cont   = true
    println("START ONLINE AGGREGATION")
    while((! l.isEmpty) && (df > 0) && cont) {
      if (s.shouldFetch(l.head.accessible_bits)) { // something can be added
        println(l.head.accessible_bits)

        s.add2(List(l.head.accessible_bits), fetch2(List(l.head)))
        //TODO: Probably gauss not required if newly added varibales are first rewritten in terms of non-basic
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
    val l = Profiler("NaivePrepare"){m.prepare(query, m.n_bits, m.n_bits)}
    Profiler("NaiveFetch"){fetch(l).map(p => p.sm)}
  }
} // end DataCube


object DataCube {
  /** creates and loads a DataCube.
      @param filename is the name of the metadata file.

      Example:
      {{{
      val dc = frontend.experiments.Tools.mkDC(10, 0.5, 1.8, 100,
        backend.ScalaBackend)
      dc.save("hello")
      core.DataCube.load("hello", backend.ScalaBackend)
      }}}
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

  def load2(filename: String, be: Backend[Payload] = CBackend.b) : DataCube = {
    val file = new File("cubedata/"+filename+"/"+filename+".dc2")
    val ois = new ObjectInputStream(new FileInputStream(file))
    val m = ois.readObject.asInstanceOf[MaterializationScheme]
    val multiCuboidLayoutData = ois.readObject.asInstanceOf[List[(List[Int],List[Boolean], List[Int], List[BigInt])]]
    ois.close
    println("MultiCuboidLayout loaded")
    val dc = new DataCube(m)
    dc.load2(be, multiCuboidLayoutData, file.getParent)

    dc
  }
}




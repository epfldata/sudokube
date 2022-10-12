//package ch.epfl.data.sudokube
package core

import backend._
import core.cube.{CuboidIndex, CuboidIndexFactory}
import core.ds.settrie.SetTrieForMoments
import core.materialization.MaterializationStrategy
import core.materialization.builder._
import core.solver._
import core.solver.lpp.{SliceSparseSolver, SparseSolver}
import core.solver.moment.Strategy._
import core.solver.moment._
import planning.NewProjectionMetaData
import util._

import java.io._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag

/** To create a DataCube, must either
  * (1) call DataCube.build(full_cube, matstrat) or
  * (2) call (companion object) DataCube.load(..)
  * before use.
  *
  * An instance of the DataCube class has a [[CuboidIndex]]
  * that stores what cuboids are materialized and tells us which ones to fetch for a given query
  *
  * The DataCube instance itself stores the Cuboids -- the data or proxies
  * to the data held in the backend.
  */
@SerialVersionUID(2L)
class DataCube(var cubeName: String = "")(implicit backend: Backend[Payload]) {

  /* protected */
  var cuboids = Array[Cuboid]()
  /** Cached moments for 0D and 1D cuboids.
      Needs to be explicitly loaded using [[DataCube.loadPrimaryMoments]]
   */
  var primaryMoments: (Long, Array[Long]) = null
  var index: CuboidIndex = null



  /**
    * Builds this cube using the base_cuboid of another DataCube
    */
  def buildFrom(that: DataCube, m: MaterializationStrategy, indexFactory: CuboidIndexFactory = CuboidIndexFactory.default, cb: CubeBuilder = CubeBuilder.default): Unit = {
    assert(m.n_bits == that.index.n_bits)
    val full_cube = that.cuboids.last
    build(full_cube, m, indexFactory, cb)
  }

  /** build each cuboid from a smallest that subsumes it -- using
    * CubeBuilder.create_build_plan().
    * has to be called explicitly, otherwise no data is created.
    * In the end, the full_cube is at position cuboids.last (this is consistent
    * with the order in [[DataCube.index]]).
    *
    * @param full_cube is integrated into the data cube as is and not copied.
    */
  def build(full_cube: Cuboid, m: MaterializationStrategy, indexFactory: CuboidIndexFactory = CuboidIndexFactory.default, cb: CubeBuilder = CubeBuilder.default) {
    assert(full_cube.n_bits == m.n_bits)
    val showProgress = m.n_bits > 25
    index = indexFactory.buildFrom(m)
    cuboids = cb.build(full_cube, m, showProgress).toArray

    val real_size = cuboids.map(c => c.size).sum
    // statistics output
    if (showProgress) {
      println("\nReal size: " + real_size + " data points ("
        + real_size.toDouble / full_cube.size.toLong + "x base cube size).")
    }
  }


  /**
    * Loads cuboids from files. Do not call this directly, but invoke the load method of the companion object.
    *
    * @param be                Backend
    * @param multicuboidLayout List of layout information, one per file. For each  file, we have
    *                          List[CuboidId], List[isSparse], List[NBits], List[NRows]
    */
  def load(multicuboidLayout: List[(List[Int], List[Boolean], List[Int], List[BigInt])], parentDir: String): Unit = {
    cuboids = new Array[Cuboid](index.length)
    implicit val ec = ExecutionContext.global
    //println(s"Reading cuboids from disk")

    val futures = multicuboidLayout.zipWithIndex.map { case ((idList, sparseList, nbitsList, sizeList), mcid) =>
      Future {
        val filename = parentDir + s"/multicube_$mcid" + backend.cuboidFileExtension
        //WARNING Converting size of cuboid from BigInt to Int
        val cmap = backend.readMultiCuboid(filename, idList.toArray, sparseList.toArray, nbitsList.toArray, sizeList.map(_.toInt).toArray)
        cmap.foreach { case (cid, cub) => cuboids(cid) = cub }
      }
    }
    Await.result(Future.sequence(futures), Duration.Inf)
  }

  /**
   * Threshold size for combining several cuboids to form a multicuboid
   */
  final val threshold = 1000 * 1000 * 200

  /**
   * Save data cube to disk. It involves 3 separate stages
   * Write CuboidIndex that contains projection metadata
   * Create a multicuboid layout that combines several cuboids to form files of sizes [[DataCube.threshold]] and write
   * this layout to another file
   * Instruct the backend to write the cuboids following the multicuboid layout
   */
  def save() {
    val filename = cubeName
    assert(!filename.isEmpty)
    val file = new File("cubedata/" + filename + "/" + filename + ".mcl")
    if (!file.exists())
      file.getParentFile.mkdirs()

    //pack cuboids upto 200 MB into a single file
    val multiCuboids = cuboids.zipWithIndex.foldLeft((Map[Int, Cuboid]() :: Nil) -> BigInt(0)) {
      case ((list, total), (cub, id)) => if ((total + cub.numBytes) < threshold) {
        ((list.head + (id -> cub)) :: list.tail) -> (total + cub.numBytes)
      } else {
        (Map(id -> cub) :: list) -> cub.numBytes
      }
    }._1

    type MultiCuboid = Map[Int, Cuboid]

    def extractCuboidData(mc: MultiCuboid) = {
      mc.foldLeft((List[Int](), List[Boolean](), List[Int](), List[BigInt]())) {
        case ((idList, sparseList, nbitsList, sizeList), (id, cub)) =>
          (id :: idList, cub.isInstanceOf[backend.SparseCuboid] :: sparseList, cub.n_bits :: nbitsList, cub.size :: sizeList)
      }
    }

    val oos = new ObjectOutputStream(new FileOutputStream(file))
    val multiCuboidLayoutData = multiCuboids.map(extractCuboidData)
    oos.writeObject(multiCuboidLayoutData)
    oos.close

    index.saveToFile(filename)
    implicit val ec = ExecutionContext.global
    println(s"Writing cuboids to disk")
    val threadTasks = multiCuboids.zipWithIndex.map { case (mc, mcid) =>
      Future {
        val filename = file.getParent + s"/multicube_$mcid" + backend.cuboidFileExtension
        //layout data is written in reverse order, so we reverse it here too
        backend.writeMultiCuboid(filename, mc.values.toArray.reverse)
      }
    }
    Await.result(Future.sequence(threadTasks), Duration.Inf)
  }


  /** This is the only place where we transfer data from C into Scala.
    */
  def fetch(pms: Seq[NewProjectionMetaData]): Array[Payload] = {
    if (cuboids.length == 0) throw new Exception("Need to build first!")
    val backend = cuboids(0).backend

    (for (pm <- pms) yield {
      val c = cuboids(pm.cuboidID).rehash_to_dense(pm.cuboidIntersection)
      //val maskString = pm.mask.mkString("")
      //println(s"Fetch mask=$maskString  maskLen = ${pm.mask.length}  origSize=${cuboids(pm.id).size}  newSize=${c.size}")
      c.asInstanceOf[backend.DenseCuboid].fetch.asInstanceOf[Array[Payload]]
    }).flatten.toArray
  }

  /** Gets rid of the Payload box. */
  def fetch2[T:ClassTag](pms: Seq[NewProjectionMetaData]
               )(implicit num: Fractional[T]): Array[T] = {
    val f1 = fetch(pms)
      f1.map(p => Util.fromLong(p.smLong))
  }

  /** Fetches a single cuboid for which we care only about the indexes where [[maskArray]] is set to true */
  def fetchWithSliceMask[T: ClassTag](pm: NewProjectionMetaData, maskArray: Array[Boolean])(implicit num: Fractional[T]): Array[T] = {
    cuboids(pm.cuboidID).rehashWithSliceAndFetch(pm.cuboidIntersection, maskArray: Array[Boolean]).map(p => Util.fromLong(p))
  }
  /** returns a solver for a given query. One needs to explicitly compute
    * and extract bounds from it. (See the code of DataCube.online_agg() for
    * an example.)
   * TODO: Generalize for all solvers
    */
  def solver[T:ClassTag](query: IndexedSeq[Int], max_fetch_dim: Int
               )(implicit num: Fractional[T]): SparseSolver[T] = {
    val l = Profiler("SolverPrepare") {
      index.prepareBatch(query)
    }
    val bounds = SolverTools.mk_all_non_neg[T](1 << query.length)

    val data = Profiler("SolverFetch") {
      fetch2(l)
    }
    Profiler("SolverConstructor") {
      new SliceSparseSolver[T](query.length, bounds, l.map(_.queryIntersection), data)
    }
  }

  /** lets us provide a callback function that is called for increasing
    bit depths of relevant projections (as provided by
    CuboidIndex.prepareOnline()).

    @param cheap_size is the number of dimensions up to which cuboids
    can be prepared for the initial iteration. cheap_size refers
    to the actual dimensionality of the cuboid, not to their
    dimensionality after projection to dimensions occurring in the
    query.

   TODO: Generalize to all solvers

    @example {{{
    import frontend.experiments.Tools._
    import core._
    import RationalTools._

    class CB {
    var bounds : Option[collection.mutable.ArrayBuffer[Interval[Rational]]] = None

    def callback(s: SparseSolver[Rational]) = {
    println(s.bounds)
    bounds = Some(s.bounds)
    true
    }
    }
    }}}
    */
  def online_agg[T:ClassTag](
    query: IndexedSeq[Int],
    cheap_size: Int,
    callback: SparseSolver[T] => Boolean, // returns whether to continue
    sliceFunc: Int => Boolean = _ => true // determines what variables to filter
  )(implicit num: Fractional[T]) {

    var prepareList = index.prepareOnline(query, cheap_size)
    val bounds = SolverTools.mk_all_non_neg[T](1 << query.length)
    val s = new SliceSparseSolver[T](query.length, bounds, List(), List(), sliceFunc)
    var df = s.df
    var cont = true
    println("START ONLINE AGGREGATION")
    val iter = prepareList.iterator
    while (iter.hasNext && (df > 0) && cont) {
      val current = iter.next()
      if (s.shouldFetch(current.queryIntersection)) { // something can be added
        println(current.queryIntersection)

        s.add2(List(current.queryIntersection), fetch2[T](List(current)))
        //TODO: Probably gauss not required if newly added varibales are first rewritten in terms of non-basic
        s.gauss(s.det_vars)
        s.compute_bounds
        cont = callback(s)
        df = s.df
      }
    }
  }

  def online_agg_moment(query: IndexedSeq[Int], cheap_size: Int, callback: MomentSolverAll[Double] => Boolean) = {
    val s = new MomentSolverAll[Double](query.size, CoMoment3)
    val prepareList = index.prepareOnline(query, cheap_size)
    val iter = prepareList.iterator
    var cont = true
    while (iter.hasNext && cont) {
      val current = iter.next()
      val fetched = fetch2[Double](List(current))
      s.add(current.queryIntersection, fetched)
      s.fillMissing()
      s.fastSolve()
      cont = callback(s)
    }
  }

  /** fetches the smallest subsuming cuboid and projects away columns not in
    * the query. Does not involve a solver.
    */
  def naive_eval(query: IndexedSeq[Int]): Array[Double] = {
    val l = Profiler("NaivePrepare") {
      index.prepareNaive(query)
    }
    //println("Naive query "+l.head.mask.sum + "  fetch = " + l.head.mask.length)
    Profiler("NaiveFetch") {
      fetch(l).map(p => p.sm)
    }
  }

  /**
   * Loads a set trie storing the moments of this data cube from disk
   * Experimental feature
   */

  def loadTrie() = {
    val filename = s"cubedata/${cubeName}_trie/${cubeName}.trie"
    val file = new File(filename)
    val in = new ObjectInputStream(new FileInputStream(file))
    in.readObject().asInstanceOf[SetTrieForMoments]
  }

  /** Saves the moments of this data cube in a trie data structure */
  def saveAsTrie() = {
    val filename = s"cubedata/${cubeName}_trie/${cubeName}.ctrie"
    val file = new File(filename)
    if (!file.exists())
      file.getParentFile.mkdirs()

    val maxsize = 1L << 30
    val dim = 25
    val be = cuboids.head.backend.asInstanceOf[OriginalCBackend]
    val cubs = index.zipWithIndex.filter(_._1.length <= dim).map { case (cols, cid) =>
      val n = cols.length
      val cuboid = cuboids(cid)
      assert(cuboid.n_bits == n)

      val be_cid = cuboid match {
        case d: be.DenseCuboid => be.denseToHybrid(d.data)
        case s: be.SparseCuboid => be.sparseToHybrid(s.data)
      }
      cols.sorted.toArray -> be_cid
    }.toArray
    be.saveAsTrie(cubs, filename, maxsize)
  }

  //TODO: FIX primary moments not necessarily saved under the same name as this cuboid
  def savePrimaryMoments(otherCubeName: String) = {
    val filename = s"cubedata/$otherCubeName/${otherCubeName}.m1s"
    val out = new ObjectOutputStream(new FileOutputStream(filename))
    out.writeObject(primaryMoments)
  }

  //TODO: FIX primary moments not necessarily saved under the same name as this cuboid
  def loadPrimaryMoments(otherCubeName: String) = {
    val filename = s"cubedata/$otherCubeName/${otherCubeName}.m1s"
    val in = new ObjectInputStream(new FileInputStream(filename))
    primaryMoments = in.readObject().asInstanceOf[(Long, Array[Long])]
  }
} // end DataCube

/**
  * DataCube that builds projections from another data cube that contains only the full cuboid.
  * Does not materialize the full cuboid again.
  * Several PartialDataCubes can share the same full cuboid.
  *
    @param cubeName the name for this data cube
  * @param basename The name of the data cube storing the full cuboid. This will be fetched at runtime.
  */
class PartialDataCube(cn: String, basename: String)(implicit val backend: Backend[Payload]) extends DataCube(cn) {
  lazy val base = DataCube.load(basename)

   def buildPartial(m: MaterializationStrategy, indexFactory: CuboidIndexFactory = CuboidIndexFactory.default, cb: CubeBuilder = CubeBuilder.default) = buildFrom(base, m, indexFactory, cb)

  override def load(multicuboidLayout: List[(List[Int], List[Boolean], List[Int], List[BigInt])], parentDir: String): Unit = {
    cuboids = new Array[Cuboid](index.length)
    implicit val ec = ExecutionContext.global
    //println(s"Reading cuboids from disk")
    val tasks = multicuboidLayout.zipWithIndex.map { case ((idList, sparseList, nbitsList, sizeList), mcid) =>
      Future {
        val filename = parentDir + s"/multicube_$mcid" + backend.cuboidFileExtension
        //WARNING Converting size of cuboid from BigInt to Int
        val cmap = backend.readMultiCuboid(filename, idList.toArray, sparseList.toArray, nbitsList.toArray, sizeList.map(_.toInt).toArray)
        cmap.foreach { case (cid, cub) => cuboids(cid) = cub }
      }
    }
    val last = cuboids.indices.last
    assert(cuboids(last) == null)
    cuboids(last) = base.cuboids.last
    Await.result(Future.sequence(tasks), Duration.Inf)
  }

  override def save(): Unit = {
    val filename = cubeName
    assert(!filename.isEmpty)
    val be = cuboids(0).backend
    val file = new File("cubedata/" + filename + "/" + filename + ".pmcl")
    if (!file.exists())
      file.getParentFile.mkdirs()

    //do not materialize base cuboid
    val multiCuboids = cuboids.dropRight(1).zipWithIndex.foldLeft((Map[Int, Cuboid]() :: Nil) -> BigInt(0)) {
      case ((list, total), (cub, id)) => if ((total + cub.numBytes) < threshold) {
        ((list.head + (id -> cub)) :: list.tail) -> (total + cub.numBytes)
      } else {
        (Map(id -> cub) :: list) -> cub.numBytes
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
    val multiCuboidLayoutData = multiCuboids.map(extractCuboidData)
    oos.writeObject(multiCuboidLayoutData)
    oos.close

    index.saveToFile(filename)
    implicit val ec = ExecutionContext.global
    println(s"Writing cuboids to disk")
    val threadTasks = multiCuboids.zipWithIndex.map { case (mc, mcid) =>
      Future {
        val filename = file.getParent + s"/multicube_$mcid" + be.cuboidFileExtension
        //layout data is written in reverse order, so we reverse it here too
        be.writeMultiCuboid(filename, mc.values.toArray.reverse)
      }
    }
    Await.result(Future.sequence(threadTasks), Duration.Inf)
  }
}

object DataCube {
  /** creates and loads a DataCube.
     @param cubeName is the name of the metadata file.
    */

  def load(cubeName: String)(implicit backend: Backend[Payload]): DataCube = {
    val file = new File("cubedata/" + cubeName + "/" + cubeName + ".mcl")
    val ois = new ObjectInputStream(new FileInputStream(file))
    //println("Loading MultiCuboidLayout...")
    val multiCuboidLayoutData = ois.readObject.asInstanceOf[List[(List[Int], List[Boolean], List[Int], List[BigInt])]]
    ois.close
    //println("MultiCuboidLayout loaded")
    val dc = new DataCube(cubeName)(backend)
    dc.index = CuboidIndexFactory.loadFromFile(cubeName)
    dc.load(multiCuboidLayoutData, file.getParent)

    dc
  }
}

object PartialDataCube {
  def load(cubeName: String, basename: String)(implicit backend : Backend[Payload]) = {
    val file = new File("cubedata/" + cubeName + "/" + cubeName + ".pmcl")
    val ois = new ObjectInputStream(new FileInputStream(file))
    //println("Loading MultiCuboidLayout...")
    val multiCuboidLayoutData = ois.readObject.asInstanceOf[List[(List[Int], List[Boolean], List[Int], List[BigInt])]]
    ois.close
    //println("MultiCuboidLayout loaded")
    val dc = new PartialDataCube(cubeName, basename)(backend)
    dc.index = CuboidIndexFactory.loadFromFile(cubeName)
    dc.load(multiCuboidLayoutData, file.getParent)
    dc
  }
}


package core.solver

import util.{BigBinary, Bits, Profiler}

import scala.collection.mutable.ListBuffer

//primary moments are calculated assuming transformer is Moment1
abstract class MomentSolver(qsize: Int, batchMode: Boolean, transformer: MomentTransformer, primaryMoments: Seq[(Int, Double)]) {
  val N = 1 << qsize
  val solverName: String
  lazy val name = solverName + transformer.name

  val moments = Profiler.profile("Moments construct") {
    Array.fill(N)(0.0)
  }
  Profiler.profile("Moments init") {
    if (transformer == Moment1Transformer)
      primaryMoments.foreach { case (i, m) => moments(i) = m }
    else if (transformer == Moment0Transformer) {
      val pmMap = primaryMoments.toMap
      pmMap.foreach {
        case (0, m) => moments(0) = m
        case (i, m) => moments(i) = pmMap(0) - m
      }
    } else {
      ???
    }
  }
  val knownSet = collection.mutable.BitSet()
  knownSet += 0
  (0 until qsize).foreach { b => knownSet += (1 << b) }

  val momentProducts = new Array[Double](N)
  val knownMomentProducts = collection.mutable.BitSet()

  Profiler.profile("ExtraMoments init") {
    buildMomentProducts()
    moments.indices.filter(!knownSet(_)).foreach { i => moments(i) = momentProducts(i) * moments(0) }
  }
  val momentsToAdd = new ListBuffer[(Int, Double)]()

  var solution = Array.fill(N)(0.0)

  def dof = N - knownSet.size

  def getStats = (dof, solution.clone())

  def buildMomentProducts() = {
    momentProducts(0) = 1.0
    knownMomentProducts.clear()
    //Set products corresponding to singleton sets
    (0 until qsize).foreach { i =>
      val j = (1 << i)
      momentProducts(j) = if (knownSet(j)) moments(j) / moments(0) else 0.5
      knownMomentProducts += j
    }
    (0 until N).foreach { i =>
      if (!knownMomentProducts(i)) {
        var s = 1
        while (s < i) {
          //find any element s subset of i
          if ((i & s) == s) {
            //products(I) = products(I \ {s}) * products({s})
            momentProducts(i) = momentProducts(i - s) * momentProducts(s)
            s = i + 1 //break
          } else {
            s = s << 1
          }
        }
        knownMomentProducts += i
      }
    }
  }

  def add(cols: Seq[Int], values: Array[Double]) = {
    val eqnColSet = Bits.toInt(cols)
    val n0 = 1 << cols.length

    val newMomentIndices = (0 until n0).map(i0 => i0 -> Bits.unproject(i0, eqnColSet)).
      filter({ case (i0, i) => !knownSet.contains(i) })

    if (newMomentIndices.size < cols.length) {
      // need less than log(n0) moments -- find individually
      newMomentIndices.foreach { case (i0, i) =>
        momentsToAdd += i -> transformer.getMoment(i0, values)
        knownSet += i
      }
    }
    else {
      //need more than log(n0) moments -- do moment transform and filter
      val cuboid_moments = transformer.getMoments(values)
      newMomentIndices.foreach { case (i0, i) =>
        momentsToAdd += i -> cuboid_moments(i0)
        knownSet += i
      }
    }
  }

  //only used in batchmode to denote end of fetch phase.
  def fillMissing()

  def solve() = {
    solution = transformer.getValues(moments)
    solution
  }
}

class CoMoment4Solver(qsize: Int, batchmode: Boolean, transformer: MomentTransformer, primaryMoments: Seq[(Int, Double)]) extends MomentSolver(qsize, batchmode, transformer, primaryMoments) {
  val qArray = Profiler.profile("qArray Construct") {
    println(s"N  = $N qsize = $qsize")
    Array.fill(qsize)(Array.fill(N)(0.0))
  }

  override val solverName: String = "Comoment4"

  override def fillMissing(): Unit = if (batchmode) fillMissingBatch() else fillMissingOnline()

  def fillMissingBatch(): Unit = {
    Profiler("FillMissingBatch") {
      Profiler.profile("AddMoments") {
        momentsToAdd.foreach { case (i, m) =>
          moments(i) = m
        }
      }
      (0 until N).foreach { i =>
        val bits = Bits.fromInt(i)
        val w = bits.length
        val parents = bits.map(b => i - (1 << b))
        Profiler("part1") {
          if (!knownSet(i)) {
            val qsum = Profiler.profile("qsum1") {
              parents.map { k => qArray(w - 1)(k) * momentProducts(i - k) }.sum
            }
            //println(s"m[$i] = $qsum")
            moments(i) = qsum
          }
        }
        Profiler("part2") {
          (w + 1 to qsize).map { j =>
            val qsum = Profiler.profile("qsum2") {
              parents.map { k => qArray(j - 1)(k) * momentProducts(i - k) }.sum
            }
            //q[S][j-1] = m[S] - 1/(j+1-|S|) sum_s [ q[S-s][j-1] ]
            val q = moments(i) - (qsum / (j - w + 1))
            //println(s"q[$i][$j] = $q")
            qArray(j - 1)(i) = q
          }
        }
      }
    }
  }

  def fillMissingOnline() = {
    Profiler("FillMissingOnline") {
      // set_size, set as int
      val toProcess = collection.mutable.SortedSet[(Int, Int)]()
      //val toProcess = collection.mutable.PriorityQueue[(Int, Int)]()
      //val toProcess2 = collection.mutable.Set[Int]()

      def children(i: Int, set0: Seq[Int]) = set0.map(b => i + (1 << b))

      val newmoments = momentsToAdd.toMap

      Profiler("PQ init") {
        toProcess ++= newmoments.keys.map(k => BigBinary(k).hamming_weight -> k)
      }

      //println("Initial set = " + toProcess.mkString(" "))
      Profiler("PQ loop") {
        while (!toProcess.isEmpty) {
          val (w, i) = toProcess.head
          toProcess -= ((w, i))
          //val (w, i) = toProcess.dequeue()

          //println(s"Process $i  ${Bits.fromInt(i).mkString("{", ",", "}")} with size $w")
          val (_, set0, set1) = Bits.hwZeroOne(i, qsize)
          val deltamom = if (knownSet(i)) {
            newmoments(i) - moments(i)
          } else {
            qArray(w - 1)(i)
          }
          moments(i) += deltamom

          Profiler("Child processing") {
            children(i, set0).foreach { c =>
              Profiler("Child q update") {
                (w + 1 to qsize).foreach { j =>
                  val deltaq = (deltamom - qArray(j - 1)(i))
                  qArray(j - 1)(c) += deltaq * momentProducts(c - i) / (j - w)
                }
              }
              val n = ((w + 1) -> c)
              Profiler("PQ update") {
                if (!toProcess.contains(n)) {
                  toProcess += n
                }
                //if(!toProcess2.contains(c)){
                //  toProcess += n
                //  toProcess2 += c
                //}
              }
            }
            (w to qsize).foreach { j => qArray(j - 1)(i) = 0 }
          }
        }
      }
      momentsToAdd.clear()
    }
  }
}


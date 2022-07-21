package core.solver.moment

import util.{BigBinary, BitUtils, Profiler}

import scala.reflect.ClassTag

class CoMoment4Solver[T:ClassTag:Fractional](qsize: Int, batchmode: Boolean, transformer: MomentTransformer[T], primaryMoments: Seq[(Int, T)]) extends MomentSolver(qsize, batchmode, transformer, primaryMoments) {
  val qArray = Profiler.profile("qArray Construct") {
    Array.fill(qsize)(Array.fill(N)(num.zero))
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
        val bits = BitUtils.IntToSet(i)
        val w = bits.length
        val parents = bits.map(b => i - (1 << b))
        Profiler("part1") {
          if (!knownSet(i)) {
            val qsum = Profiler.profile("qsum1") {
              parents.map { k =>
                num.times(qArray(w - 1)(k), momentProducts(i - k)) }.sum
            }
            //println(s"m[$i] = $qsum")
            moments(i) = qsum
          }
        }
        Profiler("part2") {
          (w + 1 to qsize).map { j =>
            val qsum = Profiler.profile("qsum2") {
              parents.map { k => num.times(qArray(j - 1)(k),momentProducts(i - k)) }.sum
            }
            //q[S][j-1] = m[S] - 1/(j+1-|S|) sum_s [ q[S-s][j-1] ]
            val q = num.minus(moments(i), num.div(qsum, num.fromInt(j - w + 1)))
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
          val (_, set0, set1) = BitUtils.hwZeroOne(i, qsize)
          val deltamom = if (knownSet(i)) {
            num.minus(newmoments(i), moments(i))
          } else {
            qArray(w - 1)(i)
          }
          moments(i) = num.plus(moments(i), deltamom)

          Profiler("Child processing") {
            children(i, set0).foreach { c =>
              Profiler("Child q update") {
                (w + 1 to qsize).foreach { j =>
                  val deltaq = num.minus(deltamom, qArray(j - 1)(i))
                  qArray(j - 1)(c) = num.plus(qArray(j - 1)(c), num.times(deltaq, num.div(momentProducts(c - i), num.fromInt(j - w))))
                }
              }
              val n = ((w + 1) -> c)
              Profiler("PQ update") {
                //if (!toProcess.contains(n)) {
                toProcess += n
                //}
                //if(!toProcess2.contains(c)){
                //  toProcess += n
                //  toProcess2 += c
                //}
              }
            }
            (w to qsize).foreach { j => qArray(j - 1)(i) = num.zero }
          }
        }
      }
      momentsToAdd.clear()
    }
  }
}

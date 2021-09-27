package core.solver

import breeze.linalg.{DenseMatrix, DenseVector, inv}
import combinatorics.Combinatorics
import util.{BigBinary, Bits, Util}

import scala.collection.mutable.ArrayBuffer

 class UniformSolver[T](val qsize: Int) (implicit num: Fractional[T]) {

   assert(qsize < 31)
   val sumValues = Util.mkAB(1 << qsize, _ => num.zero)
   val knownSums = collection.mutable.BitSet()



   def setDefaultValue(row: Int) = {
     val n = BigBinary(row).hamming_weight

     def getMeanProduct(colSet: Int) =
       Bits.fromInt(colSet).map{c =>
         num.div(sumValues(1 << c), sumValues(0))
       }.product

     //Special case for means
     val sum = if(n == 1) num.div(sumValues(0), num.fromInt(2)) else (1 to n).map { k =>

       //WARNING: Converting BigInt to Int. Ensure that query does not involve more than 30 bits
       val combs = Combinatorics.mk_comb_bi(n, k).map(i => Bits.pupInt(i.toInt, row))
       val sign = if((k % 2) == 1) num.one else num.negate(num.one)
       //TODO: Can simplify further if parents were unknown and default values were used for them
        num.times(sign, combs.map{i =>
         num.times(sumValues(row - i),  getMeanProduct(i))
       }.sum)
     }.sum
     sumValues(row) = sum
   }

   def add(cols: List[Int], values: Array[T]) = {
     val eqnColSet = Bits.toInt(cols)
     val length = cols.length
     (0 until 1 << length).foreach { i0 =>
       val i = Bits.pupInt(i0, eqnColSet)
       if (!knownSums.contains(i)) {
         val rowsToSum = (0 until 1 << length).filter(i2 => (i2 & i0) == i0)
         val rowsSum = rowsToSum.map(values(_)).sum
         knownSums += i
         sumValues(i) = rowsSum
       }
     }
   }


   def solve() = {
     val N = 1 << qsize
     val toSolve = (0 until N).filter((!knownSums.contains(_))).
       sortBy(BigBinary(_).hamming_weight)
     //println("Predicting values for " + toSolve.mkString(" "))
       toSolve.foreach { r =>
         setDefaultValue(r)
     }

     //TODO: Replace by specialized Gaussian Elimination to solve, could be cheaper

     val matA  =  DenseMatrix.zeros[Double](1 << qsize, 1 << qsize)

     (0 until N).foreach { i =>
       (0 until N).foreach{ j =>
         if( (i & j)  == i)
           matA(i, j) = 1.0
       }
     }

     val vals = DenseVector(sumValues.map(num.toDouble(_)) :_*)
     inv(matA) * vals
   }
 }

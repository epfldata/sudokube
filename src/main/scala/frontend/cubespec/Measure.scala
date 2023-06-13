package frontend.cubespec


object Aggregation extends Enumeration {
  type AggregationFunction = Value
  val SUM, COUNT, AVERAGE, VARIANCE, CORRELATION, REGRESSION = Value
}


abstract class Measure[-I, +O]() {
  val name: String
  def compute(row: I): O
}

class SingleColumnStaticMeasure(index: Int, override val name: String, mapf: String => Long) extends Measure[IndexedSeq[String], Long]() {
  override def compute(row: IndexedSeq[String]): Long = mapf(row(index))
}


class SingleColumnDynamicMeasure(colname: String, override val name: String, mapf: Object => Long) extends Measure[Map[String, Object], Long]() {
  override def compute(row: Map[String, Object]): Long = row.get(colname).map(mapf).getOrElse(0L)
}

class CountMeasure[-I]() extends Measure[I, Long] {
  override val name: String = "Count"
  override def compute(row: I) = 1L
}

class ConstantMeasure[-I](override val name: String, value: Long) extends Measure[I, Long] {
  override def compute(row: I): Long = value

}

class SquareMeasure[-I, T: Numeric](val m: Measure[I, T])(implicit val num: Numeric[T]) extends Measure[I, T] {
  override val name: String = m.name + "_square"
  override def compute(row: I): T = {
    val v = m.compute(row)
    num.times(v, v)
  }
}

class ProductMeasure[-I, T: Numeric](val m1: Measure[I, T], val m2: Measure[I, T])(implicit val num: Numeric[T]) extends Measure[I, T] {
  override val name: String = m1.name + "_times_" + m2.name
  override def compute(row: I): T = {
    val v1 = m1.compute(row)
    val v2 = m2.compute(row)
    num.times(v1, v2)
  }
}

class CompositeMeasure[-I, O](val ms: IndexedSeq[Measure[I, O]]) extends Measure[I, IndexedSeq[O]] {

  import frontend.cubespec.Aggregation._

  override val name: String = "Composite"
  val allNames = ms.map(_.name)
  override def compute(row: I): IndexedSeq[O] = ms.map(m => m.compute(row))
  def indexAndPostProcessQuery(agg: AggregationFunction, mnames: IndexedSeq[String]) = {
    val (idxes, func) = agg match {
      case SUM =>
        Vector(ms.indexWhere(x => x.name == mnames(0))) ->
          ((data: IndexedSeq[Array[Double]]) => data.head)
      case COUNT =>
        Vector(ms.indexWhere(x => x.isInstanceOf[CountMeasure[_]])) ->
          ((data: IndexedSeq[Array[Double]]) => data.head)
      case AVERAGE =>
        val cntIdx = ms.indexWhere(x => x.isInstanceOf[CountMeasure[_]])
        val sumIdx = ms.indexWhere(x => x.name == mnames(0))
        Vector(cntIdx, sumIdx) ->
          ((data: IndexedSeq[Array[Double]]) => {
            val res = data(0).indices.toArray.map { i =>
              val cnt = data(0)(i)
              val sum = data(1)(i)
              if (cnt <= 0.5) {
                //assert(sum == 0.0, s"Average sum=$sum != 0")
                0.0
              }
              else sum / cnt
            }
            res
          })
      case VARIANCE =>
        val cntIdx = ms.indexWhere(x => x.isInstanceOf[CountMeasure[_]])
        val sumIdx = ms.indexWhere(x => x.name == mnames(0))
        val sumSqIdx = ms.indexWhere(x => x.isInstanceOf[SquareMeasure[_, _]] && x.asInstanceOf[SquareMeasure[_, _]].m.name == mnames(0))
        Vector(cntIdx, sumIdx, sumSqIdx) ->
          ((data: IndexedSeq[Array[Double]]) => {
            val res = data(0).indices.toArray.map { i =>
              val cnt = data(0)(i)
              val sum = data(1)(i)
              val sumsq = data(2)(i)
              if(cnt <= 0.5) {
                //assert(sum == 0.0)
                //assert(sumsq == 0.0)
                0.0
              } else
              (cnt * sumsq - sum * sum) / (cnt * cnt)
            }
            res
          })
      case CORRELATION =>
        val cntIdx = ms.indexWhere(x => x.isInstanceOf[CountMeasure[_]])
        val sumXIdx = ms.indexWhere(x => x.name == mnames(0))
        val sumXSqIdx = ms.indexWhere(x => x.isInstanceOf[SquareMeasure[_, _]] && x.asInstanceOf[SquareMeasure[_, _]].m.name == mnames(0))
        val sumXYIdx = ms.indexWhere {
          _ match {
            case p: ProductMeasure[_, _] =>
              (p.m1.name == mnames(0) && p.m2.name == mnames(1)) ||
                (p.m1.name == mnames(1) && p.m2.name == mnames(0))
            case _ => false
          }
        }
        val sumYIdx = ms.indexWhere(x => x.name == mnames(1))
        val sumYSqIdx = ms.indexWhere(x => x.isInstanceOf[SquareMeasure[_, _]] && x.asInstanceOf[SquareMeasure[_, _]].m.name == mnames(1))
        Vector(cntIdx, sumXIdx, sumXSqIdx, sumYIdx, sumYSqIdx, sumXYIdx) ->
          ((data: IndexedSeq[Array[Double]]) => {
            val res = data(0).indices.toArray.map { i =>
              val cnt = data(0)(i)
              val sumX = data(1)(i)
              val sumX2 = data(2)(i)
              val sumY = data(3)(i)
              val sumY2 = data(4)(i)
              val sumXY = data(5)(i)
              val denom = math.sqrt((cnt * sumX2 - sumX * sumX) * (cnt * sumY2 - sumY * sumY))
              if(cnt <= 1.0) 0.0
              else if (denom == 0) 0.0
              else (cnt * sumXY - sumX * sumY) / denom
            }
            res
          })
      case REGRESSION =>
        val cntIdx = ms.indexWhere(x => x.isInstanceOf[CountMeasure[_]])
        val sumXIdx = ms.indexWhere(x => x.name == mnames(0))
        val sumXSqIdx = ms.indexWhere(x => x.isInstanceOf[SquareMeasure[_, _]] && x.asInstanceOf[SquareMeasure[_, _]].m.name == mnames(0))
        val sumXYIdx = ms.indexWhere {
          _ match {
            case p: ProductMeasure[_, _] =>
              (p.m1.name == mnames(0) && p.m2.name == mnames(1)) ||
                (p.m1.name == mnames(1) && p.m2.name == mnames(0))
            case _ => false
          }
        }
        val sumYIdx = ms.indexWhere(x => x.name == mnames(1))
        Vector(cntIdx, sumXIdx, sumXSqIdx, sumYIdx, sumXYIdx) ->
          ((data: IndexedSeq[Array[Double]]) => {
            val res = data(0).indices.toArray.map { i =>
              val cnt = data(0)(i)
              val sumX = data(1)(i)
              val sumX2 = data(2)(i)
              val sumY = data(3)(i)
              val sumXY = data(4)(i)
              val denom = (cnt * sumX2 - sumX * sumX)
              if (cnt == 0.0 || cnt == 1.0) 0.0
              else if (denom == 0) 0.0
              (cnt * sumXY - sumX * sumY) / denom
            }
            res
          })
    }
    if (idxes.contains(-1))
      throw new UnsupportedOperationException(s"$agg on ${mnames.mkString(",")} is not supported")
    else
      idxes -> func
  }
}
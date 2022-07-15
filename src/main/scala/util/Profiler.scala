package util

object Profiler {
  var startTimers = collection.mutable.Map[String, Long]()
  var durations = collection.mutable.Map[String, (Long, Long)]() withDefaultValue((0L, 0L))
  def apply[T](name: String)(func : => T): T = profile(name)(func)
  def noprofile[T](name: String)(func: => T): T = func
  def profile[T](name: String)(func: => T): T = {
    val startTime = System.nanoTime()
    val res = func
    val endTime = System.nanoTime()
    val curDur = durations(name)
    val newDur = (curDur._1 + 1, curDur._2 + (endTime - startTime))
    durations += name -> newDur
    res
  }
  /*
  def start(name: String) = {
    val end = () => {
      val et = System.nanoTime()
      val curDur = durations.getOrElse(name, (0L, 0L))
      val newDur = (curDur._1 + 1, curDur._2 + (et - startTimers(name)))
      durations += name -> newDur
    }
    startTimers += (name -> System.nanoTime())
    end
  }
*/
  def resetAll() = {
    startTimers = startTimers.empty
    durations = durations.empty
  }

  def reset(ns: Seq[String]) = {
    ns.foreach { n =>
      startTimers.remove(n)
      durations.remove(n)
    }
  }

  def padString(s: String, n : Int) = {
    val diff = n - s.length
    s + " "*diff
  }
  def print(byDuration: Boolean = true) = {
    if(!durations.isEmpty) {
      val L = durations.keys.map(_.length).max + 2
      val sorted_durations = if(byDuration) durations.toList.sortBy{_._2._2} else durations.toList.sortBy{_._1}
        sorted_durations.map{ case (n, (c, s)) => s"${padString(n, L)} :: Count = $c  Total = ${s / (1000 * 1000)} ms  Avg = ${s / (c)} ns" }.foreach(println)
    }
  }
}

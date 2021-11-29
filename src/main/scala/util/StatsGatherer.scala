package util

import java.io.{PrintStream}
import java.util.{Timer, TimerTask}

abstract class StatsGatherer[T] {
  def record: Unit

  def start: Unit

  def finish: Unit

  val stats = new collection.mutable.ArrayBuffer[(Long, Int, T)](50)
  var count = 0
  var startTime = 0L
}

class AutoStatsGatherer[T](task: => T) extends StatsGatherer[T] {
  class MyTimerTask extends TimerTask {
    override def run(): Unit = {
      record
      count match {
        //case 10 => reschedule(200)
        //case 20 => reschedule(500)
        //case 30 => reschedule(1000)
        case _ => ()
      }
    }
  }

  val timer = new Timer(true)
  var timerTask: TimerTask = null

  def reschedule(period: Int) = {
    if (timerTask != null) {
      timerTask.cancel()
      timer.purge()
    }
    timerTask = new MyTimerTask
    timer.scheduleAtFixedRate(timerTask, period, period)
  }

  override def start(): Unit = {
    startTime = System.currentTimeMillis()
    reschedule(100)
  }

  override def record() {
    count += 1
    val cur = System.currentTimeMillis()
    val stat = task
    stats += (((cur - startTime), count, stat))
  }

  override def finish() = {
    if (timerTask != null) timerTask.cancel()
    timer.purge()
    val cur = System.currentTimeMillis()
    val stat = task
    stats += (((cur - startTime), count, stat))
  }
}

class ManualStatsGatherer[T](task: => T) extends StatsGatherer[T] {
  override def start(): Unit = {
    startTime = System.nanoTime()
  }

  override def record() {
    count += 1
    val cur = System.nanoTime()
    val stat = task
    stats += (((cur - startTime), count, stat))
  }

  override def finish() = {

  }
}



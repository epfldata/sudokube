package util

import java.io.{PrintStream}
import java.util.{Timer, TimerTask}

abstract class StatsGatherer[T] {
  def record: Unit

  def start: Unit

  def finish: Unit
  final val billion = math.pow(10, 9)
  val stats = new collection.mutable.ArrayBuffer[(Double, Int, T)](50)
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
    startTime = System.nanoTime()
    reschedule(100)
  }

  override def record() {
    count += 1
    val cur = System.nanoTime()
    val stat = task
    stats += (((cur - startTime)/billion, count, stat))
  }

  override def finish() = {
    if (timerTask != null) timerTask.cancel()
    timer.purge()
    val cur = System.nanoTime()
    val stat = task
    stats += (((cur - startTime)/billion, count, stat))
  }
}

class ManualStatsGatherer[T]() extends StatsGatherer[T] {
  override def start(): Unit = {
    startTime = System.nanoTime()
  }
  var task:  Function0[T] = null
  override def record() {
    count += 1
    val cur = System.nanoTime()
    val stat = task
    stats += (((cur - startTime)/billion, count, stat()))
  }

  override def finish() = {

  }
}



package util

import java.io.{PrintStream}
import java.util.{Timer, TimerTask}

 class VaryingTimer[T](task: => T, name: String) {


   override def finalize(): Unit = {
     println(s"Destroying $name")
     super.finalize()
   }

   class MyTimerTask extends TimerTask {
     override def run(): Unit = {
       count += 1
       val cur = System.currentTimeMillis()
       val stat = task
       stats += (cur - startTime) -> stat
       count match {
         case 10 => reschedule(200)
         case 20 => reschedule(500)
         case 30 => reschedule(1000)
         case _ => ()
       }
     }
   }

   val timer = new Timer(true)
   val stats = new collection.mutable.ArrayBuffer[(Long, T)](50)
   var count = 0
   var startTime = 0L
   var timerTask: TimerTask = null

   def reschedule(period: Int) = {
     if (timerTask != null) {
       timerTask.cancel()
       timer.purge()
     }
     timerTask = new MyTimerTask
     timer.scheduleAtFixedRate(timerTask, period, period)
   }

   def start(): Unit = {
     startTime = System.currentTimeMillis()
     reschedule(100)
   }

   def finish() = {
     timerTask.cancel()
     timer.purge()
     val cur = System.currentTimeMillis()
     val stat = task
     stats += (cur - startTime) -> stat
   }

}



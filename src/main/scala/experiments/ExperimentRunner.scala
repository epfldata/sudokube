package experiments

import java.io.{File, FileOutputStream, PrintStream}
import java.net.InetAddress
import java.time.{Duration, LocalDateTime}

trait ExperimentRunner {
  def run_expt(func: String => (String, Int) => Unit)(args: Array[String]) = {
    val liftedargs = args.lift
    val param = liftedargs(0).getOrElse("noarg")
    val timestamp = liftedargs(1).getOrElse(Experiment.now())
    val numIters = liftedargs(2).map(_.toInt).getOrElse(10)

    val fullexptname = getClass.getCanonicalName + ": " + param
    val hostname = InetAddress.getLocalHost().getHostName()
    val logfile = new File(s"expdata/logs/$timestamp/$hostname.log")
    if (!logfile.exists()) logfile.getParentFile.mkdirs()
    val fileout = new PrintStream(new FileOutputStream(logfile, true))
    val startTime = LocalDateTime.now()
    println(s"Host:$hostname time:$startTime")
    fileout.println(s"Starting $fullexptname at $startTime")
    func(param)(timestamp, numIters)
    val endTime = LocalDateTime.now()
    val minutes = Duration.between(startTime, endTime).getSeconds / 60.0
    fileout.println(s"Finished $fullexptname at $endTime (iterations=$numIters time=$minutes min)")
  }
}

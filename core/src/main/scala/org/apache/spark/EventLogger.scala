package org.apache.spark

import java.io.{File, FileOutputStream}

import org.apache.spark.scheduler._
import org.apache.spark.scheduler.SparkListenerRDDCreation
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.scheduler.SparkListenerJobStart
import com.google.common.io.Files

class EventLogger(eventLogPath: String) extends SparkListener with Logging {
  val logFile = new File(eventLogPath)
  val stream = new EventLogOutputStream(new FileOutputStream(logFile))
  var replayer: Option[EventReplayer] = None

  private[this] def logEvent(event: SparkListenerEvents) {
    replayer.foreach(_.appendEvent(event))
    stream.writeObject(event)
  }

  private[spark] def close() {
    stream.close()
  }

  private[spark] def registerEventReplayer(replayer: EventReplayer) {
    // Flush the log file, so that the replayer can see the most recent events
    stream.flush()
    this.replayer = Some(replayer)
  }

  def saveEventLogAs(path: String) {
    Files.copy(logFile, new File(path))
  }

  override def onStageCompleted(stageCompleted: StageCompleted) { }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) { }

  override def onTaskStart(taskStart: SparkListenerTaskStart) { }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult) { }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) { }

  override def onJobStart(jobStart: SparkListenerJobStart) { }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) { }

  override def onRDDCreation(rddCreation: SparkListenerRDDCreation) {
    logEvent(rddCreation)
  }
}

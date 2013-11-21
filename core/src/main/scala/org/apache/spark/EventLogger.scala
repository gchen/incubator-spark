package org.apache.spark

import java.io.{File, FileOutputStream}

import org.apache.spark.scheduler._
import org.apache.spark.scheduler.SparkListenerRDDCreation
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.scheduler.SparkListenerJobStart

class EventLogger(eventLogPath: String) extends SparkListener with Logging {
  val stream = new EventLogOutputStream(new FileOutputStream(new File(eventLogPath)))
  var replayer: Option[EventReplayer] = None

  private[this] def logEvent(event: SparkListenerEvents) {
    stream.writeObject(event)
    replayer.foreach(_.appendEvent(event))
  }

  private[spark] def close() {
    stream.close()
  }

  private[spark] def registerEventReplayer(replayer: EventReplayer) {
    this.replayer = Some(replayer)
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

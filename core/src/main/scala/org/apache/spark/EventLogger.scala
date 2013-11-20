package org.apache.spark

import java.io.{ObjectOutputStream, File, FileOutputStream}

import org.apache.spark.scheduler._
import org.apache.spark.scheduler.SparkListenerRDDCreation
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.scheduler.SparkListenerJobStart

class EventLogger(eventLogPath: String) extends SparkListener with Logging {
  val stream = new ObjectOutputStream(new FileOutputStream(new File(eventLogPath)))

  private[this] def logEvent(event: SparkListenerEvents) {
    stream.writeObject(event)
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

package org.apache.spark

import java.io.{File, FileOutputStream}

import org.apache.spark.scheduler._
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.scheduler.SparkListenerJobStart
import com.google.common.io.Files

class EventLogger(eventLogPath: String) extends SparkListener with Logging {
  val logFile = new File(eventLogPath)

  var stream: Option[EventLogOutputStream] =
    Some(new EventLogOutputStream(new FileOutputStream(logFile)))

  var replayer: Option[EventReplayer] = None

  private[this] def logEvent(event: SparkListenerEvents) {
    replayer.foreach(_.appendEvent(event))
    stream.synchronized {
      stream.foreach(_.writeObject(event))
    }
  }

  private[spark] def close() {
    stream.synchronized {
      stream.foreach(_.close())
      stream = None
    }
  }

  private[spark] def registerEventReplayer(replayer: EventReplayer) {
    stream.synchronized {
      // Flush the log file, so that the replayer can see the most recent events
      stream.foreach(_.flush())
      this.replayer = Some(replayer)
    }
  }

  def saveEventLogAs(path: String) {
    stream.synchronized {
      stream.foreach(_.flush())
      Files.copy(logFile, new File(path))
    }
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart) {
    logEvent(taskStart)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    logEvent(taskEnd)
  }

  override def onJobStart(jobStart: SparkListenerJobStart) {
    logEvent(jobStart)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    logEvent(jobEnd)
  }
}

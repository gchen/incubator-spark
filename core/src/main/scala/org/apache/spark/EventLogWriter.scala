package org.apache.spark

import java.io.{File, FileOutputStream}

import scala.collection.immutable.HashSet
import scala.collection.mutable

private[spark] class EventLogWriter(eventLogPath: String) extends Logging {
  private[this] val eventLog = new EventLogOutputStream(new FileOutputStream(new File(eventLogPath)))

  def logEvent(entry: EventLogEntry) {
    eventLog.writeObject(entry)
  }

  def flush() {
    eventLog.flush()
  }

  def close() {
    eventLog.close()
  }
}

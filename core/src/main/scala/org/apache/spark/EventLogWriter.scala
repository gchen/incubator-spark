package org.apache.spark

import java.io.{File, FileOutputStream}
import scala.collection.immutable.HashSet
import scala.collection.mutable

class EventLogWriter extends Logging {
  private[this] var eventLog: Option[EventLogOutputStream] = _
  private[this] var eventLogReader: Option[EventLogReader] = None

  initEventLog(Option(DebuggerOptions.logPath))

  def initEventLog(eventLogPath: Option[String]) {
    eventLog = for {
      path <- eventLogPath
      file = new File(path)
      if !file.exists
    } yield new EventLogOutputStream(new FileOutputStream(file))
  }

  val checksums = new mutable.HashMap[Any, HashSet[ChecksumEvent]]
  val checksumMismatches = new mutable.ArrayBuffer[ChecksumEvent]

  def log(entry: EventLogEntry) {
    for (log <- eventLog)
      log.writeObject(entry)

    for (reader <- eventLogReader)
      reader.appendEvent(entry)

    entry match {
      case c: ChecksumEvent => processChecksumEvent(c)
      case _ => ()
    }
  }

  def flush() {
    for (log <- eventLog)
      log.flush()
  }

  def stop() {
    for (log <- eventLog)
      log.close()
  }

  private[spark] def registerEventLogReader(reader: EventLogReader) {
    eventLogReader = Some(reader)
  }

  private[spark] def processChecksumEvent(c: ChecksumEvent) {
    if (checksums.contains(c.key)) {
      if (!checksums(c.key).contains(c)) {
        if (checksums(c.key).exists(_.mismatch(c)))
          reportChecksumMismatch(c)
        checksums(c.key) += c
      }
    } else {
      checksums.put(c.key, HashSet(c))
    }
  }

  private def reportChecksumMismatch(c: ChecksumEvent) {
    checksumMismatches += c
    logWarning(c.warningString)
  }
}

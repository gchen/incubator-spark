package org.apache.spark

import java.io.{File, FileOutputStream}

import scala.collection.immutable.HashSet
import scala.collection.mutable

import akka.actor.{Address, Actor, Props, ActorSystem}
import akka.pattern._
import akka.util.{Duration, Timeout}
import akka.dispatch.Await

private[spark] trait EventLogWriter {
  def flush() {}
  def stop() {}
  def log(entry: EventLogEntry) {}
  def setEventLogPath(path: String) {}
  val checksumMismatches: Seq[ChecksumEvent]
}

private[spark] class EventLogWriterImpl(
    isDriver: Boolean,
    driverHost: String,
    driverPort: Int,
    actorSystem: ActorSystem)
  extends EventLogWriter with Logging {

  class EventLogWriterActor extends Actor {
    def receive = {
      case LogEvent(entry) => log(entry)
      case Flush => flush()
      case Stop => context.stop(self)
    }

    def log(entry: EventLogEntry) {
      eventLog.foreach(_.writeObject(entry))

      for (reader <- eventLogReader)
        reader.appendEvent(entry)

      entry match {
        case c: ChecksumEvent => processChecksumEvent(c)
        case _ => ()
      }
    }

    def flush() {
      eventLog.foreach(_.flush())
    }

    def stop() {
      eventLog.foreach(_.close())
    }
  }

  private[this] var eventLog: Option[EventLogOutputStream] = None

  private[this] var eventLogReader: Option[EventLogReader] = _

  private[this] val logWriterActor = isDriver match {
    case true =>
      actorSystem.actorOf(Props[EventLogWriterActor], "eventLogWriter")

    case false =>
      val address = Address("akka", "spark", driverHost, driverPort)
      actorSystem.actorFor(address.toString + "/user/eventLogWriter")
  }

  override def setEventLogPath(eventLogPath: String) {
    eventLog = Some(new EventLogOutputStream(new FileOutputStream(new File(eventLogPath))))
  }

  val checksums = new mutable.HashMap[Any, HashSet[ChecksumEvent]]
  val checksumMismatches = new mutable.ArrayBuffer[ChecksumEvent]

  override def log(entry: EventLogEntry) {
    logWriterActor ! LogEvent(entry)
  }

  override def flush() {
    logWriterActor ! Flush
  }

  override def stop() {
    implicit val timeout = Timeout.never
    Await.result(logWriterActor ? Stop, Duration.Inf)
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

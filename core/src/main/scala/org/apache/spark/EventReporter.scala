package org.apache.spark

import akka.actor._
import akka.dispatch.Await
import akka.pattern._
import akka.util.{Timeout, Duration}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler._

sealed trait EventReporterMessage
case class LogEvent(entry: EventLogEntry) extends EventReporterMessage
case object Flush extends EventReporterMessage
case object Stop extends EventReporterMessage

trait EventReporter {
  val actorSystem: ActorSystem

  def start(eventLogPath: String)

  def stop()

  def flush()

  def logEvent(entry: EventLogEntry)

  def reportAssertionFailure(failure: AssertionFailure) {
    logEvent(failure)
  }

  def reportException(exception: Throwable, task: Task[_]) {
    logEvent(ExceptionEvent(exception, task))
  }

  def reportLocalException(exception: Throwable, task: Task[_]) {
    logEvent(ExceptionEvent(exception, task))
  }

  def reportRDDCreation(rdd: RDD[_], location: Array[StackTraceElement]) {
    logEvent(RDDCreation(rdd, location))
  }

  def reportTaskSubmission(tasks: Seq[Task[_]]) {
    logEvent(TaskSubmission(tasks))
  }

  def registerEventLogReader(reader: EventLogReader)
}

class ExecutorSideEventReporter(
    val actorSystem: ActorSystem,
    driverHost: String,
    driverPort: Int)
  extends EventReporter with Logging {

  val driverSideActor: ActorRef = actorSystem.actorFor(eventLogWriterActorPath)

  private[this] def eventLogWriterActorPath = {
    Address("akka", "spark", driverHost, driverPort) + "/user/eventLogWriter"
  }

  def start(eventLogPath: String) {
  }

  def stop() {
  }

  def flush() {
    driverSideActor ! Flush
  }

  override def logEvent(entry: EventLogEntry) {
    driverSideActor ! LogEvent(entry)
  }

  def registerEventLogReader(reader: EventLogReader) {
  }
}

class EventReporterActor(reporter: EventReporter) extends Actor {
  def receive = {
    case LogEvent(entry) =>
      reporter.logEvent(entry)

    case Flush =>
      reporter.flush()

    case Stop =>
      context.stop(self)
      sender ! 'OK
  }
}

class DriverSideEventReporter(
    val actorSystem: ActorSystem,
    driverHost: String,
    driverPort: Int)
  extends EventReporter with Logging {

  private[this] val eventReporterActor =
    actorSystem.actorOf(Props(new EventReporterActor(this)), "eventReporter")

  private[this] var eventLogWriter: Option[EventLogWriter] = None

  private[this] var eventLogReader: Option[EventLogReader] = None

  def logEvent(entry: EventLogEntry) {
    eventLogReader.foreach(_.appendEvent(entry))
    eventLogWriter.foreach(_.logEvent(entry))
  }

  def start(eventLogPath: String) {
    eventLogWriter = Some(new EventLogWriter(eventLogPath))
  }

  def stop() {
    // TODO is it necessary to make this duration configurable?
    val duration = Duration(5, "seconds")
    implicit val timeout = new Timeout(duration)
    Await.result(eventReporterActor ? Stop, duration)
    eventLogWriter.foreach(_.close())
    eventLogWriter = None
    eventLogReader = None
  }

  def flush() {
    eventLogWriter.foreach(_.flush())
  }

  def registerEventLogReader(reader: EventLogReader) {
    eventLogReader = Some(reader)
  }
}

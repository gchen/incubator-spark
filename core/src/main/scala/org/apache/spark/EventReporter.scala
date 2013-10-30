package org.apache.spark

import akka.actor._
import akka.pattern._
import akka.util.Timeout
import akka.util.duration._
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler._
import org.apache.spark.util.Utils
import scala.util.MurmurHash
import java.nio.ByteBuffer
import akka.dispatch.Await

sealed trait EventReporterMessage
case class LogEvent(entry: EventLogEntry) extends EventReporterMessage
case class StopEventReporter() extends EventReporterMessage

class EventReporterActor(eventLogWriter: EventLogWriter) extends Actor with Logging {
  override def receive = {
    case LogEvent(entry) =>
      eventLogWriter.log(entry)

    case StopEventReporter =>
      eventLogWriter.stop()
      sender ! 'OK
  }
}

class EventReporter(
    driverHost: String,
    driverPort: Int,
    isDriver: Boolean,
    actorSystem: ActorSystem)
  extends Logging {

  var debuggerEnabled = DebuggerOptions.enabled
  var checksumEnabled = DebuggerOptions.checksum

  var eventLogWriter = if (isDriver && debuggerEnabled) Some(new EventLogWriter) else None
  var reporterActor = initReporterActor()

  def initReporterActor() = (debuggerEnabled, isDriver) match {
    case (true, true) =>
      for (writer <- eventLogWriter) yield
        actorSystem.actorOf(Props(new EventReporterActor(writer)), "event-reporter")

    case (true, false) => {
      val address = Address("akka", "spark", driverHost, driverPort)
      Some(actorSystem.actorFor(address.toString + "/user/event-reporter"))
    }

    case _ => None
  }

  def reportAssertionFailure(failure: AssertionFailure) {
    for (actor <- reporterActor)
      actor ! LogEvent(failure)
  }

  def reportException(exception: Throwable, task: Task[_]) {
    for (actor <- reporterActor)
      actor ! LogEvent(ExceptionEvent(exception, task))
  }

  def reportLocalException(exception: Throwable, task: Task[_]) {
    for (writer <- eventLogWriter)
      writer.log(ExceptionEvent(exception, task))
  }

  def reportRDDCreation(rdd: RDD[_], location: Array[StackTraceElement]) {
    for (writer <- eventLogWriter)
      writer.log(RDDCreation(rdd, location))
  }

  def reportTaskSubmission(tasks: Seq[Task[_]]) {
    for (writer <- eventLogWriter)
      writer.log(TaskSubmission(tasks))
  }

  def reportTaskChecksum(task: Task[_], result: DirectTaskResult[_], serializedResult: ByteBuffer) {
    if (checksumEnabled) {
      val checksum = new MurmurHash[Byte](42)

      task match {
        case r: ResultTask[_, _] =>
          for (byte <- serializedResult.array())
            checksum(byte)

          val serializedFunc = Utils.serialize(r.func)
          val funcChecksum = new MurmurHash[Byte](42)

          for (byte <- serializedFunc)
            funcChecksum(byte)

          for (actor <- reporterActor)
            actor ! LogEvent(ResultTaskChecksum(
                r.rdd.id, r.partitionId, funcChecksum.hash, checksum.hash))

        case s: ShuffleMapTask =>
          val serializedAccumUpdates = Utils.serialize(result.accumUpdates)

          for (byte <- serializedAccumUpdates)
            checksum(byte)

          for (actor <- reporterActor)
            actor ! LogEvent(ShuffleMapTaskChecksum(s.rdd.id, s.partitionId, checksum.hash))

        case _ =>
          logWarning("Unknown task type: " + task)
      }
    }
  }

  def reportShuffleChecksum(rdd: RDD[_], partition: Int, outputSplit: Int, checksum: Int) {
    for (actor <- reporterActor)
      actor ! LogEvent(ShuffleOutputChecksum(rdd.id, partition, outputSplit, checksum))
  }

  def stop() {
    for (actor <- reporterActor) {
      implicit val timeout = Timeout(30 seconds)
      Await.result(actor ? StopEventReporter, timeout.duration)
    }

    eventLogWriter = None
    reporterActor = None
  }
}

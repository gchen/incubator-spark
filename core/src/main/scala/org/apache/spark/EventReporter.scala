package org.apache.spark

import akka.actor._
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler._
import org.apache.spark.util.Utils
import scala.util.MurmurHash
import java.nio.ByteBuffer

sealed trait EventReporterMessage
case class LogEvent(entry: EventLogEntry) extends EventReporterMessage
case object Flush extends EventReporterMessage
case object Stop extends EventReporterMessage

class EventReporter(
    driverHost: String,
    driverPort: Int,
    isDriver: Boolean,
    actorSystem: ActorSystem)
  extends Logging {

  var debuggerEnabled = DebuggerOptions.enabled

  var checksumEnabled = DebuggerOptions.checksum

  val eventLogWriter =
    if (debuggerEnabled)
      new EventLogWriterImpl(isDriver, driverHost, driverPort, actorSystem)
    else
      new EventLogWriter {
        val checksumMismatches = Seq()
      }

  if (DebuggerOptions.logPath != null) {
    setEventLogPath(DebuggerOptions.logPath)
  }

  def setEventLogPath(path: String) {
    setEventLogPath(path)
  }

  def reportAssertionFailure(failure: AssertionFailure) {
    log(failure)
  }

  def reportException(exception: Throwable, task: Task[_]) {
    log(ExceptionEvent(exception, task))
  }

  def reportLocalException(exception: Throwable, task: Task[_]) {
    log(ExceptionEvent(exception, task))
  }

  def reportRDDCreation(rdd: RDD[_], location: Array[StackTraceElement]) {
    log(RDDCreation(rdd, location))
  }

  def reportTaskSubmission(tasks: Seq[Task[_]]) {
    log(TaskSubmission(tasks))
  }

  private[this] def log(entry: EventLogEntry) {
    eventLogWriter.log(entry)
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

          log(ResultTaskChecksum(r.rdd.id, r.partitionId, funcChecksum.hash, checksum.hash))

        case s: ShuffleMapTask =>
          val serializedAccumUpdates = Utils.serialize(result.accumUpdates)

          for (byte <- serializedAccumUpdates)
            checksum(byte)

          log(ShuffleMapTaskChecksum(s.rdd.id, s.partitionId, checksum.hash))

        case _ =>
          logWarning("Unknown task type: " + task)
      }
    }
  }

  def reportShuffleChecksum(rdd: RDD[_], partition: Int, outputSplit: Int, checksum: Int) {
    log(ShuffleOutputChecksum(rdd.id, partition, outputSplit, checksum))
  }

  def stop() {
    eventLogWriter.stop()
  }
}

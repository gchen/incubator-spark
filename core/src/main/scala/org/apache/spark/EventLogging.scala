package org.apache.spark

import org.apache.spark.scheduler.Task
import org.apache.spark.rdd.RDD

sealed trait EventLogEntry

case class ExceptionEvent(exception: Throwable, task: Task[_]) extends EventLogEntry
case class RDDCreation(rdd: RDD[_], location: Array[StackTraceElement]) extends EventLogEntry
case class TaskSubmission(tasks: Seq[Task[_]]) extends EventLogEntry

sealed trait ChecksumEvent extends EventLogEntry {
  def key: Any
  def mismatch(other: ChecksumEvent): Boolean
  def warningString: String

  def rddId: Int
  def partition: Int
  def checksum: Int
}

case class ShuffleMapTaskChecksum(
    rddId: Int,
    partition: Int,
    checksum: Int)
  extends ChecksumEvent {

  def key = (rddId, partition)

  def mismatch(other: ChecksumEvent) = other match {
    case ShuffleMapTaskChecksum(r, p, otherChecksum) =>
      (rddId, partition) == (r, p) && checksum != otherChecksum

    case _ =>
      false
  }

  def warningString =
    "Nondeterminism detected in accumulator updates for ShuffleMapTask on RDD %d, partition %d"
      .format(rddId, partition)
}

case class ResultTaskChecksum(
    rddId: Int,
    partition: Int,
    funcHash: Int,
    checksum: Int)
  extends ChecksumEvent {

  def key = (rddId, partition, funcHash)

  def mismatch(other: ChecksumEvent) = other match {
    case ResultTaskChecksum(r, p, f, otherChecksum) =>
      (rddId, partition, funcHash) == (r, p, f) && checksum != otherChecksum

    case _ =>
      false
  }

  def warningString =
    "Nondeterminism detected in ResultTask on RDD %d, partition %d".format(rddId, partition)
}

case class ShuffleOutputChecksum(
    rddId: Int,
    partition: Int,
    outputSplit: Int,
    checksum: Int)
  extends ChecksumEvent {

  def key = (rddId, partition, outputSplit)

  def mismatch(other: ChecksumEvent) = other match {
    case ShuffleOutputChecksum(r, p, o, otherChecksum) =>
      (rddId, partition, outputSplit) == (r, p, o) && checksum != otherChecksum

    case _ =>
      false
  }

  def warningString =
    "Nondeterminism detected in shuffle output on RDD %d, partition %d, output split %d"
      .format(rddId, partition, outputSplit)
}

sealed trait AssertionFailure extends EventLogEntry
case class ElementAssertionFailure(rddId: Int, element: Any) extends AssertionFailure
case class ReduceAssertionFailure(rddId: Int, splitIndex: Int, element: Any)
  extends AssertionFailure

import java.io.{ObjectInputStream, ObjectOutputStream, ObjectStreamClass}
import java.io.{InputStream, OutputStream}

class EventLogOutputStream(out: OutputStream) extends ObjectOutputStream(out)

class EventLogInputStream(
    in: InputStream,
    val context: SparkContext)
  extends ObjectInputStream(in) {

  override def resolveClass(desc: ObjectStreamClass) =
    Class.forName(desc.getName, false, Thread.currentThread.getContextClassLoader)
}

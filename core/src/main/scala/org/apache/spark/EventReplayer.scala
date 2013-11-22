package org.apache.spark

import java.io.{EOFException, PrintWriter, File, FileInputStream}

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable
import org.apache.spark.scheduler.SparkListenerRDDCreation
import org.apache.spark.scheduler.SparkListenerTaskStart
import scala.Some

class EventReplayer(context: SparkContext, var eventLogPath: String = null) {
  private[this] val stream = {
    if (eventLogPath == null) {
      eventLogPath = System.getProperty("spark.eventLogging.eventLogPath")

      require(eventLogPath != null, "Please specify the event log path, " +
        "either by setting the \"spark.eventLogging.eventLogPath\", " +
        "or by constructing EventReplayer with a legal event log path")
    }

    new EventLogInputStream(new FileInputStream(new File(eventLogPath)), context)
  }

  private[this] val _events = new ArrayBuffer[SparkListenerEvents]

  private[this] val _rdds = new ArrayBuffer[RDD[_]]

  private[this] val rddIdToCanonical = new mutable.HashMap[Int, Int]

  context.eventLogger.foreach(_.registerEventReplayer(this))
  loadEvents()

  def rdds = _rdds.readOnly

  def events = _events.readOnly

  def tasks = for (SparkListenerTaskStart(task, _) <- events) yield task

  def tasksForRDD(rdd: RDD[_]): Seq[Task[_]] =
    for {
      task <- tasks
      taskRDD <- task match {
        case t: ResultTask[_, _] => Some(t.rdd)
        case t: ShuffleMapTask => Some(t.rdd)
        case _ => None
      }
      if taskRDD.id == rdd.id
    } yield task

  def taskWithId(stageId: Int, partition: Int): Option[Task[_]] =
    (for {
      task <- tasks
      (taskStageId, taskPartition) <- task match {
        case t: ResultTask[_, _] => Some((t.stageId, t.partitionId))
        case t: ShuffleMapTask => Some((t.stageId, t.partitionId))
        case _ => None
      }
      if taskStageId == stageId && taskPartition == partition
    } yield task).headOption

  private[this] def loadEvents() {
    try {
      while (true) {
        val event = stream.readObject().asInstanceOf[SparkListenerEvents]
        appendEvent(event)
        // TODO Lian: According to Arthur, should check for checksum mismatch here, but I doubt...
      }
    }
    catch {
      case _: EOFException => stream.close()
    }
  }

  /**
   * All task failures caused by some unhandled exception.
   */
  def exceptionFailures() =
    for {
      SparkListenerTaskEnd(task, reason, info, _) <- events
      exceptionFailure <- reason match {
        case r: ExceptionFailure => Some(r)
        case _ => None
      }
    } yield (task, exceptionFailure)

  private[spark] def appendEvent(event: SparkListenerEvents) {
    _events += event
    event match {
      case SparkListenerRDDCreation(rdd, _) =>
        _rdds += rdd
        rddIdToCanonical(rdd.id) = rdd.id

      case _ =>
    }
  }

  private[this] def rddType(rdd: RDD[_]) = rdd.getClass.getSimpleName

  /**
   * Visualizes the RDD DAG with GraphViz
   *
   * @param format Output file format, can be "pdf", "png", "svg", etc., default to "pdf".  Please
   *               refer to GraphViz documentation for all supported file formats.
   * @return The absolution file path of the output file
   */
  def visualizeRDDs(format: String = "pdf") = {
    val dotFile = File.createTempFile("spark-rdds-", "")
    val dot = new PrintWriter(dotFile)

    dot.println("digraph {")

    for (SparkListenerRDDCreation(rdd, trace) <- events) {
      dot.print("%d [label=\"%d %s\"]".format(rdd.id, rdd.id, rddType(rdd)))
      for (dep <- rdd.dependencies) {
        dot.println("  %d -> %d;".format(rdd.id, dep.rdd.id))
      }
    }

    dot.println("}")
    dot.close()

    val dotFilePath = dotFile.getAbsolutePath
    Runtime.getRuntime.exec("dot -Grankdir=BT -T%s %s -o %s.%s"
      .format(format, dotFilePath, dotFilePath, format)).waitFor()

    dotFilePath + "." + format
  }

  def printRDDs() {
    for (SparkListenerRDDCreation(rdd, trace) <- events) {
      println("#%d: %-20s %s".format(rdd.id, rddType(rdd), firstExternalElement(trace)))
    }
  }

  private[this] def firstExternalElement(trace: Array[StackTraceElement]) =
    trace.tail.find(_.getClass.getPackage == context.getClass.getPackage)
      .getOrElse(trace.headOption.getOrElse(""))
}

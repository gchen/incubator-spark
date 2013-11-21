package org.apache.spark

import java.io.{EOFException, PrintWriter, File, FileInputStream}

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListenerRDDCreation, SparkListenerEvents}
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

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

  def appendEvent(event: SparkListenerEvents) {
    _events += event
    event match {
      case SparkListenerRDDCreation(rdd, _) =>
        _rdds += rdd
        rddIdToCanonical(rdd.id) = rdd.id

      case _ =>
    }
  }

  private[this] def rddType(rdd: RDD[_]) = rdd.getClass.getSimpleName

  def visualizeRDDs() = {
    val file = File.createTempFile("spark-rdds-", "")
    val dot = new PrintWriter(file)

    dot.println("digraph {")

    for (SparkListenerRDDCreation(rdd, trace) <- events) {
      dot.print("%d [label=\"%d %s\"]".format(rdd.id, rdd.id, rddType(rdd)))
      for (dep <- rdd.dependencies) {
        dot.println("  %d -> %d;".format(rdd.id, dep.rdd.id))
      }
    }

    dot.println("}")
    dot.close()

    val path = file.getAbsolutePath
    Runtime.getRuntime.exec("dot -Grankdir=BT -Tpdf %s -o %s.pdf".format(path, path))
    path + ".pdf"
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

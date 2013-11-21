package org.apache.spark

import java.io.{PrintWriter, File, FileInputStream}

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListenerRDDCreation, SparkListenerEvents}
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

class EventReplayer(context: SparkContext, eventLogPath: String) {
  private[this] val stream =
    new EventLogInputStream(new FileInputStream(new File(eventLogPath)), context)

  private[this] val _events = new ArrayBuffer[SparkListenerEvents]

  private[this] val _rdds = new ArrayBuffer[RDD[_]]

  private[this] val rddIdToCanonical = new mutable.HashMap[Int, Int]

  context.eventLogger.foreach(_.registerEventReplayer(this))
  loadEvents()

  def rdds = _rdds.readOnly

  def events = _events.readOnly

  private[this] def loadEvents() {
    while (stream.available() > 0) {
      val event = stream.readObject().asInstanceOf[SparkListenerEvents]
      appendEvent(event)
      // TODO Lian: According to Arthur, should check for checksum mismatch here, but I doubt...
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

  def visualizeRDDs(): String = {
    def rddType(rdd: RDD[_]) = rdd.getClass.getSimpleName

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
}

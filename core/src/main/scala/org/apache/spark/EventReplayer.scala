package org.apache.spark

import java.io.{EOFException, PrintWriter, File, FileInputStream}

import org.apache.spark.rdd.{ForallAssertionRDD, RDD}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.SparkListenerTaskStart
import org.apache.spark.util.Utils.~>
import scala.collection.immutable
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class EventReplayer(context: SparkContext, var eventLogPath: String = null) {
  private[this] val stream = {
    if (eventLogPath == null) {
      eventLogPath = System.getProperty("spark.eventLogging.eventLogPath")

      require(
        eventLogPath != null,
        "Please specify the event log path, " +
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

  private[this] def collectJobRDDs(job: ActiveJob) {
    def collectJobStages(stage: Stage, visited: Set[Stage]): Set[Stage] =
      if (!visited.contains(stage)) {
        val newVisited = visited + stage
        val ancestorStages = for {
          parent <- stage.parents.toSet[Stage]
          ancestor <- collectJobStages(parent, newVisited)
        } yield ancestor
        ancestorStages + stage
      }
      else {
        Set.empty[Stage]
      }

    def collectStageRDDs(rdd: RDD[_], visited: Set[RDD[_]]): Set[RDD[_]] =
      if (!visited.contains(rdd)) {
        val newVisited = visited + rdd
        val ancestorRDDs = for {
          dep <- rdd.dependencies.toSet[Dependency[_]]
          ancestor <- dep match {
            case _: ShuffleDependency[_, _] => Set.empty[RDD[_]]
            case _ => collectStageRDDs(dep.rdd, newVisited)
          }
        } yield ancestor
        ancestorRDDs + rdd
      }
      else {
        Set.empty[RDD[_]]
      }

    for {
      stage <- collectJobStages(job.finalStage, Set.empty[Stage])
      rdd <- collectStageRDDs(stage.rdd, Set.empty[RDD[_]])
    } {
      _rdds += rdd
      rddIdToCanonical(rdd.id) = rdd.id
    }
  }

  private[spark] def appendEvent(event: SparkListenerEvents) {
    _events += event

    event match {
      case SparkListenerJobStart(job, _) =>
        // Records all RDDs within the job
        collectJobRDDs(job)

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

    for (rdd <- rdds) {
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
    for (rdd <- rdds) {
      println("#%d: %-20s".format(rdd.id, rddType(rdd)))
    }
  }

  private def replace[T: ClassManifest](rdd: RDD[T], newRDD: RDD[T]) {
    val canonicalId = rddIdToCanonical(rdd.id)
    _rdds(canonicalId) = newRDD
    rddIdToCanonical(newRDD.id) = canonicalId

    for (descendantRddIndex <- (canonicalId + 1) until _rdds.length) {
      val updatedRDD = _rdds(descendantRddIndex).dependenciesUpdated(new (RDD ~> RDD) {
        def apply[U](dependency: RDD[U]) = {
          _rdds(rddIdToCanonical(dependency.id)).asInstanceOf[RDD[U]]
        }
      })

      _rdds(descendantRddIndex) = updatedRDD
      rddIdToCanonical(updatedRDD.id) = descendantRddIndex
    }
  }

  def assert[T: ClassManifest](rdd: RDD[T], assertion: T => Boolean): RDD[T] = {
    val rddId = rdd.id
    val assertionRDD = new ForallAssertionRDD(rdd, { (element: T, partition: Partition) =>
      if (assertion(element))
        None
      else
        Some(SparkListenerAssertionFailure(rddId, partition.index, element))
    })
    replace(rdd, assertionRDD)
    assertionRDD
  }
}

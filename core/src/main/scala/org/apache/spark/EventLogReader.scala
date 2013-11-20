package org.apache.spark

import java.io.{File, FileInputStream}

import org.apache.spark.rdd.RDD
import org.apache.spark.util._
import org.apache.spark.scheduler.{Task, ResultTask, ShuffleMapTask}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class EventLogReader(sc: SparkContext, eventLogPath: Option[String] = None) extends Logging {
  /** The `ObjectInputStream` created from the event log, from which events are loaded */
  private[this] val objectInputStream = initObjectInputStream()

  private def initObjectInputStream() =
    for {
      path <- eventLogPath orElse Option(DebuggerOptions.logPath)
      file = new File(path)
      if file.exists
    } yield new EventLogInputStream(new FileInputStream(file), sc)

  /** List of RDDs indexed by their canonical ID */
  private[this] val _rdds = new ArrayBuffer[RDD[_]]

  /** Map of RDD IDs to canonical RDD IDs (reverse of `_rdds`) */
  private[this] val rddIdToCanonical = new mutable.HashMap[Int, Int]

  /** Events loaded from the event log. */
  val events = new ArrayBuffer[EventLogEntry]

  loadEvents()
  sc.env.eventReporter.registerEventLogReader(this)

  def rdds = _rdds.readOnly

  /** Prints a human readable RDD list */
  def printRDDs() {
    for (RDDCreation(rdd, location) <- events)
      println("#%02d: %-20s %s".format(rdd.id, rddType(rdd), firstExternalElement(location)))
  }

  /** List all tasks */
  def tasks: Seq[Task[_]] =
    for {
      TaskSubmission(tasks) <- events
      task <- tasks
    } yield task

  /** Finds all the tasks that were run to compute the given RDD */
  def tasksForRDD(rdd: RDD[_]): Seq[Task[_]] =
    for {
      task <- tasks

      taskRDD <- task match {
        case r: ResultTask[_, _] => Some(r.rdd)
        case s: ShuffleMapTask => Some(s.rdd)
        case _ => None
      }

      if taskRDD.id == rdd.id
    } yield task

  /** Finds the task with given stage ID and partition */
  def taskWithId(stageId: Int, partition: Int): Option[Task[_]] = {
    val candidates = for {
      task <- tasks

      (taskStageId, taskPartition) <- task match {
        case r: ResultTask[_, _] => Some((r.stageId, r.partitionId))
        case s: ShuffleMapTask => Some((s.stageId, s.partitionId))
        case _ => None
      }

      if (taskStageId, taskPartition) == (stageId, partition)
    } yield task

    candidates.headOption
  }

  def loadEvents() {
    for (in <- objectInputStream) {
      while (in.available() > 0) {
        appendEvent(in.readObject.asInstanceOf[EventLogEntry])
      }
    }
  }

  /**
   * Append an event to the events collection.  All RDDs are extracted from `RDDCreation` events and stored.
   *
   * @param event The event to be appended
   */
  private[spark] def appendEvent(event: EventLogEntry) {
    events += event

    event match {
      case RDDCreation(rdd, location) =>
        sc.updateRddId(rdd.id)
        _rdds += rdd
        rddIdToCanonical(rdd.id) = rdd.id

      case _ => ()
    }
  }

  def replace[T: ClassManifest](rdd: RDD[T], newRDD: RDD[T]) {
    val canonicalId = rddIdToCanonical(rdd.id)

    _rdds(canonicalId) = newRDD
    rddIdToCanonical(newRDD.id) = canonicalId

    for (descendantRddIndex <- (canonicalId + 1) until _rdds.length) {
      val updatedRDD = _rdds(descendantRddIndex).mapDependencies(new (RDD ~> RDD) {
        def apply[U](dependency: RDD[U]): RDD[U] =
          _rdds(rddIdToCanonical(dependency.id)).asInstanceOf[RDD[U]]
      })

      _rdds(descendantRddIndex) = updatedRDD
      rddIdToCanonical(updatedRDD.id) = descendantRddIndex
    }
  }

  private def rddType(rdd: RDD[_]) =
    rdd.getClass.getName.replaceFirst("""^spark\.""", "")

  private def firstExternalElement(location: Array[StackTraceElement]) = {
    val first = location.tail.find(!_.getClassName.matches("""spark\.[A-Z].*"""))
    first.orElse(location.headOption).getOrElse("")
  }
}

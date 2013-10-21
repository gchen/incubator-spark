package org.apache.spark

import com.google.common.io.Files
import java.io.File
import org.scalatest.FunSuite

class EventLoggingSuite extends FunSuite {
  def initializeEventLogging(
      sc: SparkContext,
      eventLog: File,
      debuggerEnabled: Boolean,
      checksumEnabled: Boolean) {
    val reporter: EventReporter = sc.env.eventReporter

    reporter.debuggerEnabled = debuggerEnabled
    reporter.checksumEnabled = checksumEnabled

    for (writer <- reporter.eventLogWriter)
      writer.initEventLog(Some(eventLog.getAbsolutePath))
  }

  def makeSparkContextWithoutEventLogging() = new SparkContext("local", "test")

  def makeSparkContext(
      eventLog: File,
      debuggerEnabled: Boolean = true,
      checksumEnabled: Boolean = true) = {
    val sc = makeSparkContextWithoutEventLogging()
    initializeEventLogging(sc, eventLog, debuggerEnabled, checksumEnabled)
    sc
  }

  test("restore ParallelCollection from log") {
    val eventLog = new File(Files.createTempDir(), "event.log")

    {
      val sc = makeSparkContext(eventLog)
      sc.makeRDD(1 to 4)
      sc.stop()
    }

    val sc = makeSparkContext(eventLog)
    val r = new EventLogReader(sc, Some(eventLog.getAbsolutePath))

    assert(r.rdds.length === 1)
    assert(r.rdds(0).collect().toList === (1 to 4).toList)

    sc.stop()
  }

  test("interactive event log reading") {
    val eventLog = new File(Files.createTempDir(), "event.log")

    {
      val sc = makeSparkContext(eventLog)
      sc.makeRDD(1 to 4)

      for (writer <- sc.env.eventReporter.eventLogWriter)
        writer.flush()
    }

    val sc = makeSparkContextWithoutEventLogging()
    val r = new EventLogReader(sc, Some(eventLog.getAbsolutePath))

    assert(r.rdds.length === 1)
    assert(r.rdds(0).collect().toList === (1 to 4).toList)
  }
}

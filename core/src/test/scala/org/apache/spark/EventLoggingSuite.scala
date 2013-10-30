package org.apache.spark

import com.google.common.io.Files
import java.io.File
import org.scalatest.FunSuite

class EventLoggingSuite extends FunSuite with LocalSparkContext with Logging {
  initLogging()

  def initEventLogging(sc: SparkContext, eventLog: File, debuggerEnabled: Boolean, checksumEnabled: Boolean) {
    val reporter = sc.env.eventReporter

    reporter.debuggerEnabled = debuggerEnabled
    reporter.checksumEnabled = checksumEnabled

    for (writer <- reporter.eventLogWriter)
      writer.initEventLog(Some(eventLog.getAbsolutePath))
  }

  def localSpark = new SparkContext("local", "test")

  test("restore ParallelCollection from log") {
    val eventLog = new File(Files.createTempDir(), "event.log")

    LocalSparkContext.withSpark(localSpark)(sc => {
      initEventLogging(sc, eventLog, debuggerEnabled = true, checksumEnabled = true)
      sc.makeRDD(1 to 4)

      val reader = new EventLogReader(sc, Some(eventLog.getAbsolutePath))
      assert(reader.rdds.length === 1)
      assert(reader.rdds(0).collect().toList === (1 to 4).toList)
    })
  }

  test("interactive event log reading") {
    val eventLog = new File(Files.createTempDir(), "event.log")

    LocalSparkContext.withSpark(localSpark)(sc => {
      initEventLogging(sc, eventLog, debuggerEnabled = true, checksumEnabled = true)
      sc.makeRDD(1 to 4)

      for (writer <- sc.env.eventReporter.eventLogWriter)
        writer.flush()
    })

    LocalSparkContext.withSpark(localSpark)(sc => {
      val reader = new EventLogReader(sc, Some(eventLog.getAbsolutePath))
      assert(reader.rdds.length === 1)
      assert(reader.rdds(0).collect().toList === (1 to 4).toList)
    })
  }
}

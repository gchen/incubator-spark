package org.apache.spark

import com.google.common.io.Files
import java.io.File
import org.scalatest.FunSuite

class EventLoggingSuite extends FunSuite with LocalSparkContext with Logging {
  var eventLog: File = _

  override def beforeEach() {
    eventLog = new File(Files.createTempDir(), "event.log")
  }

  override def afterEach() {
    eventLog.delete()
  }

  private def enableEventLogging(
      sc: SparkContext,
      eventLog: File,
      debuggerEnabled: Boolean = true,
      checksumEnabled: Boolean = true) {

    val reporter = sc.env.eventReporter

    reporter.debuggerEnabled = debuggerEnabled
    reporter.checksumEnabled = checksumEnabled
    reporter.eventLogWriter.setEventLogPath(eventLog.getAbsolutePath)
  }

  private def withLocalSpark(f: SparkContext => Unit) =
    LocalSparkContext.withSpark(new SparkContext("local", "test"))(f)

  test("restore ParallelCollection from log") {
    // Make an RDD
    withLocalSpark { sc =>
      enableEventLogging(sc, eventLog)
      sc.makeRDD(1 to 4)
    }

    // Read the RDD back from the event log
    withLocalSpark { sc =>
      enableEventLogging(sc, eventLog)
      val r = new EventLogReader(sc, Some(eventLog.getAbsolutePath))
      assert(r.rdds.length === 1)
      assert(r.rdds(0).collect().toList === (1 to 4).toList)
    }
  }

  test("interactive event log reading") {
    withLocalSpark { sc1 =>
      enableEventLogging(sc1, eventLog)

      // Make an RDD
      sc1.makeRDD(1 to 4)
      SparkEnv.get.eventReporter.eventLogWriter.flush()

      // TODO Remove this once Typesafe Config is used for Spark configuration
      // This is a workaround to avoid the inner `SparkContext` binds to the same port used by the outer one.
      System.clearProperty("spark.driver.port")

      withLocalSpark { sc2 =>
        // Read the RDD back from the event log
        val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
        assert(r.rdds.length === 1)
        assert(r.rdds(0).collect().toList === (1 to 4).toList)

        // Make an RDD
        SparkEnv.set(sc1.env)
        sc1.makeRDD(1 to 5)
        SparkEnv.get.eventReporter.eventLogWriter.flush()

        // Read the RDD back from the event log
        SparkEnv.set(sc2.env)
        r.loadEvents()
        assert(r.rdds.length === 2)
        assert(r.rdds(1).collect().toList === (1 to 5).toList)
      }
    }
  }

  test("set nextRddId after restoring") {
    withLocalSpark { sc =>
      enableEventLogging(sc, eventLog)

      // Make 2 RDDs
      sc.makeRDD(1 to 4).map(_ + 1)
    }

    withLocalSpark { sc =>
      // Read them back from the event log
      val r = new EventLogReader(sc, Some(eventLog.getAbsolutePath))
      assert(r.rdds.length === 2)

      val n = sc.makeRDD(1 to 5)
      assert(n.id != r.rdds(0).id)
      assert(n.id != r.rdds(1).id)
    }
  }

  test("checksum verification") {
    var collected: List[Double] = null

    // Make some RDD that has non-deterministic transformation
    withLocalSpark { sc =>
      enableEventLogging(sc, eventLog)
      collected = sc.makeRDD(1 to 4).map(_ => math.random).collect().toList
    }

    var writer: EventLogWriter = null

    // Read the RDDs back and check for non-determinism
    withLocalSpark { sc =>
      val r = new EventLogReader(sc, Some(eventLog.getAbsolutePath))

      assert(r.rdds.length === 2)
      assert(r.rdds(0).collect().toList === (1 to 4).toList)
      assert(r.rdds(1).collect().toList != collected)

      writer = sc.env.eventReporter.eventLogWriter
    }

    // Make sure we found some checksum mismatch
    assert(writer.checksumMismatches.nonEmpty)
  }
}

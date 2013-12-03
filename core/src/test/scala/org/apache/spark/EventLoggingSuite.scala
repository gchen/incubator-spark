package org.apache.spark

import java.io._
import java.util.concurrent.{TimeUnit, CountDownLatch}

import akka.util.duration._
import org.apache.spark.rdd.ParallelCollectionRDD
import org.apache.spark.scheduler._
import org.scalatest.FunSuite

class DummyException extends Exception

class EventLoggingSuite extends FunSuite with LocalSparkContext {
  import LocalSparkContext.withSpark

  var eventLogFile: File = _

  val AwaitTimeout = 10.seconds.toMillis

  override def beforeEach() {
    eventLogFile = File.createTempFile("spark-event-", ".log")
    enableEventLogging()
  }

  override def afterEach() {
    eventLogFile.delete()
    disableEventLogging()
  }

  def withLocalSpark[T](f: SparkContext => T) =
    withSpark(new SparkContext("local", "test"))(f)

  def enableEventLogging() {
    System.setProperty("spark.eventLogging.enabled", "true")
    System.setProperty("spark.eventLogging.eventLogPath", eventLogFile.getAbsolutePath)
  }

  def disableEventLogging() {
    System.clearProperty("spark.eventLogging.enabled")
    System.clearProperty("spark.eventLogging.eventLogPath")
  }

  def runJob(sc: SparkContext)(job: SparkContext => Unit) {
    val (listener, latch) = addJobEndListener(sc)
    try {
      job(sc)
    } finally {
      assert(awaitJobEnd(sc, listener, latch))
    }
  }

  def makeReplayer(sc: SparkContext) = {
    new EventReplayer(sc, eventLogFile.getAbsolutePath)
  }

  def addJobEndListener(sc: SparkContext) = {
    val eventProcessed = new CountDownLatch(1)
    val listener = new SparkListener {
      override def onJobEnd(jobEnd: SparkListenerJobEnd) {
        eventProcessed.countDown()
      }
    }

    sc.addSparkListener(listener)
    (listener, eventProcessed)
  }

  def awaitJobEnd(sc: SparkContext, listener: SparkListener, latch: CountDownLatch) = {
    val success = latch.await(AwaitTimeout, TimeUnit.MILLISECONDS)
    sc.removeSparkListener(listener)
    success
  }

  test("EventLogger should log events to log file") {
    withLocalSpark { sc =>
      runJob(sc) {
        _.makeRDD(1 to 3).collect()
      }

      val replayer = makeReplayer(sc)
      assert(replayer.rdds.size === 1)

      val rdd = replayer.rdds(0)
      assert(rdd.isInstanceOf[ParallelCollectionRDD[_]])
      assert(rdd.context === sc)
    }
  }

  test("EventLogger should receive new events from EventLogger") {
    withLocalSpark { sc =>
      val replayer = makeReplayer(sc)

      // No events were written in the event log yet
      assert(replayer.rdds.size === 0)

      runJob(sc) {
        _.makeRDD(1 to 3).collect()
      }

      // One RDD created
      assert(replayer.rdds.size === 1)

      val jobStartEvent = replayer.events.count {
        case _: SparkListenerJobStart => true
        case _ => false
      }

      // One job started
      assert(jobStartEvent === 1)

      val rdd = replayer.rdds(0)
      assert(rdd.isInstanceOf[ParallelCollectionRDD[_]])
      assert(rdd.context === sc)
    }
  }

  test("RDD restored from event log can be reused") {
    withLocalSpark { sc =>
      runJob(sc) {
        _.makeRDD(1 to 3).map(_ * 2).collect()
      }

      // Restores RDDs and re-run the job
      val replayer = makeReplayer(sc)
      val rdd = replayer.rdds(1)
      val r = rdd.collect()
      assert(r.toList === List(2, 4, 6))
    }
  }

  test("Load event log from another session") {
    withLocalSpark { sc =>
      runJob(sc) { sc =>
        sc.makeRDD(1 to 3).map(_ * 2).collect()
      }
    }

    disableEventLogging()

    // Simulates another Spark shell session with event logging disabled
    withLocalSpark { sc =>
      // Restores RDDs and re-run the job
      val replayer = makeReplayer(sc)
      val resultRdd = replayer.rdds(1)
      assert(resultRdd.partitions != null)
      assert(resultRdd.collect().toList === List(2, 4, 6))
    }
  }

  ignore("EventReplayer.visualizeRDDs should generate a PDF file") {
    withLocalSpark { sc =>
      runJob(sc) { sc =>
        // Creates a complex RDD DAG
        val r1 = sc.makeRDD(1 to 3)
        val r2 = r1.map(_ * 2)
        val r3 = sc.makeRDD(2 to 4)
        val r4 = r1.cartesian(r3)
        val r5 = r4.map { case (a, b) => a + b }
        val r6 = r2.zip(r5)
        r6.collect()
      }

      // Restores RDDs and re-run the job
      val replayer = makeReplayer(sc)
      val pdf = new File(replayer.visualizeRDDs())
      assert(pdf.exists())
      pdf.delete()
    }
  }

  test("Task events should be written into event log") {
    withLocalSpark { sc =>
      runJob(sc) { sc =>
        sc.makeRDD(1 to 4, 2).map(_ * 2).collect()
      }

      val replayer = makeReplayer(sc)
      val taskEvents = replayer.events.collect {
        case event: SparkListenerTaskStart => event
      }

      assert(taskEvents.size === 2)
    }
  }

  test("Exception failures should be recorded") {
    withLocalSpark { sc =>
      runJob(sc) { sc =>
        intercept[SparkException] {
          sc.makeRDD(1 to 4, 2).map(_ => throw new DummyException()).collect()
        }
      }

      val replayer = makeReplayer(sc)
      val failures = replayer.taskEndReasons.values.collect {
        case failure: ExceptionFailure => failure
      }

      assert(failures.size > 0)

      for (ExceptionFailure(className, _, _, _) <- failures) {
        assert(className === classOf[DummyException].getName)
      }
    }
  }

  test("Restore RDDs with wide dependencies") {
    withLocalSpark { sc =>
      var expected: Any = null

      runJob(sc) { sc =>
        expected = sc.makeRDD(1 to 4).groupBy {
          n => if (n % 2 == 0) (0, n) else (1, n)
        }.collect()
      }

      val replayer = makeReplayer(sc)

      // 4 RDDs in total:
      // - makeRDD: ParallelCollectionRDD
      // - groupBy: MappedRDD, ShuffledRDD & MapPartitionsWithContextRDD
      assert(replayer.rdds.size === 4)
      assert(replayer.rdds(3).collect() === expected)
    }
  }

  test("assertForall should detect evil RDD element") {
    withLocalSpark { sc =>
      runJob(sc) {
        _.makeRDD(1 to 4, 2).collect()
      }

      val replayer = makeReplayer(sc)

      runJob(sc) { sc =>
        val rdd = replayer.rdds(0)
        val rddWithAssertion = replayer.assertForall[Int](rdd)(_ % 2 == 0)
        intercept[SparkException] {
          rddWithAssertion.collect()
        }
      }
    }
  }

  test("assertForall should keep silient if assertion holds") {
    withLocalSpark { sc =>
      runJob(sc) {
        _.makeRDD(1 to 4, 2).collect()
      }

      val replayer = makeReplayer(sc)

      runJob(sc) { sc =>
        val rdd = replayer.rdds(0)
        val rddWithAssertion = replayer.assertForall[Int](rdd)(_ => true)
        assert(rddWithAssertion.collect().sameElements(1 to 4))
      }
    }
  }

  test("assertExists should detect evil RDD") {
    withLocalSpark { sc =>
      runJob(sc) {
        _.makeRDD(1 to 5 by 2, 2).collect()
      }

      val replayer = makeReplayer(sc)

      runJob(sc) { sc =>
        val rdd = replayer.rdds(0)
        val rddWithAssertion = replayer.assertExists[Int](rdd)(_ % 2 == 0)
        intercept[SparkException] {
          rddWithAssertion.collect()
        }
      }
    }
  }

  test("assertExists should keep silient if assertion holds") {
    withLocalSpark { sc =>
      runJob(sc) {
        _.makeRDD(1 to 4, 2).collect()
      }

      val replayer = makeReplayer(sc)

      runJob(sc) { sc =>
        val rdd = replayer.rdds(0)
        val rddWithAssertion = replayer.assertExists[Int](rdd)(_ > 0)
        assert(rddWithAssertion.collect().sameElements(1 to 4))
      }
    }
  }
}

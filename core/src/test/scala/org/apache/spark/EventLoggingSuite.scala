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

  test("A SparkListenerJobStart event should be posted when a job is started") {
    disableEventLogging()

    withLocalSpark { sc =>
      val eventPosted = new CountDownLatch(1)

      sc.addSparkListener(new SparkListener {
        override def onJobStart(jobStart: SparkListenerJobStart) {
          eventPosted.countDown()
        }
      })

      sc.makeRDD(1 to 3).collect()
      assert(eventPosted.await(AwaitTimeout, TimeUnit.MILLISECONDS))
    }
  }

  test("EventLogger should log events to log file") {
    withLocalSpark { sc =>
      val eventProcessed = new CountDownLatch(1)

      // Add a dummy listener to the end of the SparkListenerBus to indicate
      // that all listeners have been called.
      sc.addSparkListener(new SparkListener {
        override def onJobEnd(jobEnd: SparkListenerJobEnd) {
          eventProcessed.countDown()
        }
      })

      sc.makeRDD(1 to 3).collect()

      // Wait until the EventLogger.onJobStart is finally called
      assert(eventProcessed.await(AwaitTimeout, TimeUnit.MILLISECONDS))

      val replayer = new EventReplayer(sc, eventLogFile.getAbsolutePath)
      assert(replayer.rdds.size === 1)

      val rdd = replayer.rdds.head
      assert(rdd.isInstanceOf[ParallelCollectionRDD[_]])
      assert(rdd.context === sc)
    }
  }

  test("EventLogger should receive new events from EventLogger") {
    withLocalSpark { sc =>
      val eventProcessed = new CountDownLatch(1)

      // Add a dummy listener to the end of the SparkListenerBus to indicate
      // that all listeners have been called.
      sc.addSparkListener(new SparkListener {
        override def onJobEnd(jobEnd: SparkListenerJobEnd) {
          eventProcessed.countDown()
        }
      })

      val replayer = new EventReplayer(sc, eventLogFile.getAbsolutePath)

      // No events are written in the event log yet
      assert(replayer.rdds.size === 0)

      sc.makeRDD(1 to 3).collect()
      assert(eventProcessed.await(AwaitTimeout, TimeUnit.MILLISECONDS))

      assert(replayer.rdds.size === 1)

      val jobStartEvent = replayer.events.count {
        case event: SparkListenerJobStart => true
        case _ => false
      }
      assert(jobStartEvent === 1)

      val rdd = replayer.rdds.head
      assert(rdd.isInstanceOf[ParallelCollectionRDD[_]])
      assert(rdd.context === sc)
    }
  }

  test("RDD restored from event log can be reused") {
    withLocalSpark { sc =>
      implicit val actorSystem = sc.env.actorSystem
      val allEventsProcessed = new CountDownLatch(1)

      // Add a dummy listener to the end of the SparkListenerBus to indicate
      // that all listeners have been called.
      val listener = new SparkListener {
        override def onJobEnd(jobEnd: SparkListenerJobEnd) {
          allEventsProcessed.countDown()
        }
      }

      sc.addSparkListener(listener)
      sc.makeRDD(1 to 3)
        .map(_ * 2)
        .collect()

      // Wait until the EventLogger.onJobEnd is finally invoked
      assert(allEventsProcessed.await(AwaitTimeout, TimeUnit.MILLISECONDS))
      sc.removeSparkListener(listener)

      // Restore RDDs and re-run the job
      val replayer = new EventReplayer(sc, eventLogFile.getAbsolutePath)
      val rdd = replayer.rdds(1)
      val r = rdd.collect()
      assert(r.toList === List(2, 4, 6))
    }
  }

  test("Load event log from another session") {
    withLocalSpark { sc =>
      val allEventsProcessed = new CountDownLatch(1)

      // Add a dummy listener to the end of the SparkListenerBus to indicate
      // that all listeners have been called.
      val listener = new SparkListener {
        override def onJobEnd(jobEnd: SparkListenerJobEnd) {
          allEventsProcessed.countDown()
        }
      }

      sc.addSparkListener(listener)
      val r1 = sc.makeRDD(1 to 3)
      val r2 = r1.map(_ * 2)
      r2.collect()

      // Wait until the EventLogger.onJobEnd is finally invoked
      assert(allEventsProcessed.await(AwaitTimeout, TimeUnit.MILLISECONDS))
      sc.removeSparkListener(listener)
    }

    disableEventLogging()

    // Simulate another Spark shell session with event logging disabled
    withLocalSpark { sc =>
      // Restore RDDs and re-run the job
      val replayer = new EventReplayer(sc, eventLogFile.getAbsolutePath)
      val resultRdd = replayer.rdds(1)
      assert(resultRdd.partitions != null)
      assert(resultRdd.collect().toList === List(2, 4, 6))
    }
  }

  test("EventReplayer.visualizeRDDs should generate a PDF file") {
    withLocalSpark { sc =>
      implicit val actorSystem = sc.env.actorSystem
      val allEventsProcessed = new CountDownLatch(1)

      // Add a dummy listener to the end of the SparkListenerBus to indicate
      // that all listeners have been called.
      val listener = new SparkListener {
        override def onJobEnd(jobEnd: SparkListenerJobEnd) {
          allEventsProcessed.countDown()
        }
      }

      sc.addSparkListener(listener)
      val r1 = sc.makeRDD(1 to 3)
      val r2 = r1.map(_ * 2)
      val r3 = sc.makeRDD(2 to 4)
      val r4 = r1.cartesian(r3).map { case (a, b) => a + b }
      val r5 = r2.zip(r4)
      r5.collect()

      // Wait until the EventLogger.onJobEnd is finally invoked
      assert(allEventsProcessed.await(AwaitTimeout, TimeUnit.MILLISECONDS))
      sc.removeSparkListener(listener)

      // Restore RDDs and re-run the job
      val replayer = new EventReplayer(sc, eventLogFile.getAbsolutePath)
      val pdf = new File(replayer.visualizeRDDs())
      assert(pdf.exists())
      pdf.delete()
    }
  }

  test("Task events should be written into event log") {
    withLocalSpark { sc =>
      val allEventsProcessed = new CountDownLatch(1)

      // Add a dummy listener to the end of the SparkListenerBus to indicate
      // that all listeners have been called.
      val listener = new SparkListener {
        override def onJobEnd(jobEnd: SparkListenerJobEnd) {
          allEventsProcessed.countDown()
        }
      }

      sc.addSparkListener(listener)
      sc.makeRDD(1 to 4, 2)
        .map(_ * 2)
        .collect()

      // Wait until the EventLogger.onJobEnd is finally invoked
      assert(allEventsProcessed.await(AwaitTimeout, TimeUnit.MILLISECONDS))
      sc.removeSparkListener(listener)

      val replayer = new EventReplayer(sc, eventLogFile.getAbsolutePath)
      val taskEvents = replayer.events.collect {
        case event: SparkListenerTaskStart => event
      }

      assert(taskEvents.size === 2)
    }
  }

  test("Exception failures should be recorded") {
    withLocalSpark { sc =>
      val allEventsProcessed = new CountDownLatch(1)

      // Add a dummy listener to the end of the SparkListenerBus to indicate
      // that all listeners have been called.
      val listener = new SparkListener {
        override def onJobEnd(jobEnd: SparkListenerJobEnd) {
          allEventsProcessed.countDown()
        }
      }

      sc.addSparkListener(listener)

      intercept[SparkException] {
        sc.makeRDD(1 to 4, 2)
          .map(_ => throw new DummyException())
          .collect()
      }

      // Wait until the EventLogger.onJobEnd is finally invoked
      assert(allEventsProcessed.await(AwaitTimeout, TimeUnit.MILLISECONDS))
      sc.removeSparkListener(listener)

      val replayer = new EventReplayer(sc, eventLogFile.getAbsolutePath)
      val failures = replayer.exceptionFailures()

      assert(failures.size > 0)

      for ((task, ExceptionFailure(className, _, _, _)) <- failures) {
        assert(className === classOf[DummyException].getName)
      }
    }
  }

  test("Restore RDDs with wide dependencies") {
    withLocalSpark { sc =>
      val allEventsProcessed = new CountDownLatch(1)

      val listener = new SparkListener {
        override def onJobEnd(jobEnd: SparkListenerJobEnd) {
          allEventsProcessed.countDown()
        }
      }

      sc.addSparkListener(listener)

      val r1 = sc.makeRDD(1 to 4)
      val r2 = r1.groupBy(n => if (n % 2 == 0) (0, n) else (1, n))
      val expected = r2.collect()

      assert(allEventsProcessed.await(AwaitTimeout, TimeUnit.MILLISECONDS))
      sc.removeSparkListener(listener)

      val replayer = new EventReplayer(sc, eventLogFile.getAbsolutePath)

      // 4 RDDs in total:
      // - makeRDD: ParallelCollectionRDD
      // - groupBy: MappedRDD
      //            ShuffledRDD
      //            MapPartitionsWithContextRDD
      assert(replayer.rdds.size === 4)
      assert(replayer.rdds.last.collect() === expected)
    }
  }
}

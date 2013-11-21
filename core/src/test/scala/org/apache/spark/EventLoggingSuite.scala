package org.apache.spark

import org.scalatest.FunSuite
import org.apache.spark.scheduler.{SparkListenerJobEnd, SparkListener, SparkListenerRDDCreation}
import akka.dispatch.{Await, Promise}
import akka.util.duration._
import org.apache.spark.rdd.EmptyRDD
import java.io._

class EventLoggingSuite extends FunSuite with LocalSparkContext {
  import LocalSparkContext.withSpark

  var eventLogFile: File = _

  val AwaitTimeout = 10.seconds

  override def beforeEach() {
    eventLogFile = File.createTempFile("spark-event-", ".log")
  }

  override def afterEach() {
    eventLogFile.delete()
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

  test("A SparkListenerRDDCreation event should be posted when an RDD is created") {
    withLocalSpark { sc =>
      implicit val actorSystem = sc.env.actorSystem
      val eventPosted = Promise[Unit]()

      sc.addSparkListener(new SparkListener {
        override def onRDDCreation(rddCreation: SparkListenerRDDCreation) {
          eventPosted.success(())
        }
      })

      new EmptyRDD[Unit](sc)
      assert(Await.result(eventPosted.future, AwaitTimeout) === ())
    }
  }

  test("EventLogger should log events to log file") {
    enableEventLogging()

    withLocalSpark { sc =>
      implicit val actorSystem = sc.env.actorSystem
      val eventProcessed = Promise[Unit]()

      // Add a dummy listener to the end of the SparkListenerBus to indicate
      // that all listeners have been called.
      sc.addSparkListener(new SparkListener {
        override def onRDDCreation(rddCreation: SparkListenerRDDCreation) {
          eventProcessed.success(())
        }
      })

      // Create a new RDD, a SparkListenerRDDCreation event should be written
      new EmptyRDD[Unit](sc)

      // Wait until the EventLogger is finally called
      assert(Await.result(eventProcessed.future, AwaitTimeout) === ())

      val replayer = new EventReplayer(sc, eventLogFile.getAbsolutePath)
      assert(replayer.rdds.size === 1)

      val rdd = replayer.rdds.head
      assert(rdd.isInstanceOf[EmptyRDD[Unit]])
      assert(rdd.context === sc)
    }
  }

  test("EventLogger should receive new events from EventLogger") {
    enableEventLogging()

    withLocalSpark { sc =>
      implicit val actorSystem = sc.env.actorSystem
      val eventProcessed = Promise[Unit]()

      // Add a dummy listener to the end of the SparkListenerBus to indicate
      // that all listeners have been called.
      sc.addSparkListener(new SparkListener {
        override def onRDDCreation(rddCreation: SparkListenerRDDCreation) {
          eventProcessed.success(())
        }
      })

      val replayer = new EventReplayer(sc, eventLogFile.getAbsolutePath)

      // No events are written in the event log
      assert(replayer.rdds.size === 0)

      // Create a new RDD, a SparkListenerRDDCreation event should be appended to the replayer
      new EmptyRDD[Unit](sc)

      // Wait until the EventLogger is finally called
      assert(Await.result(eventProcessed.future, AwaitTimeout) === ())

      assert(replayer.rdds.size === 1)
      val rdd2 = replayer.rdds.head
      assert(rdd2.isInstanceOf[EmptyRDD[Unit]])
      assert(rdd2.context === sc)
    }
  }

  test("RDD restored from event log can be used as usual") {
    enableEventLogging()

    withLocalSpark { sc =>
      implicit val actorSystem = sc.env.actorSystem
      val allEventsProcessed = Promise[Unit]()

      // Add a dummy listener to the end of the SparkListenerBus to indicate
      // that all listeners have been called.
      val listener = new SparkListener {
        override def onJobEnd(jobEnd: SparkListenerJobEnd) {
          allEventsProcessed.success(())
        }
      }

      sc.addSparkListener(listener)
      val r1 = sc.makeRDD(1 to 3)
      val r2 = r1.map(_ * 2)
      r2.collect()

      // Wait until the EventLogger is finally called
      assert(Await.result(allEventsProcessed.future, AwaitTimeout) === ())
      sc.removeSparkListener(listener)

      // Restore RDDs and re-run the job
      val replayer = new EventReplayer(sc, eventLogFile.getAbsolutePath)
      val result = replayer.rdds(1)
      assert(result.collect().toList === List(2, 4, 6))
    }
  }
}

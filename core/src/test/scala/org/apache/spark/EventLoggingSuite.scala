package org.apache.spark

import org.scalatest.FunSuite
import org.apache.spark.scheduler.SparkListener
import akka.dispatch.{Await, Promise}
import akka.util.duration._
import org.apache.spark.rdd.EmptyRDD
import java.io._
import org.apache.spark.scheduler.SparkListenerRDDCreation

class EventLoggingSuite extends FunSuite with LocalSparkContext {
  import LocalSparkContext.withSpark

  var eventLogFile: File = _

  val AwaitTimeout = 10.seconds

  override def beforeEach() {
    eventLogFile = File.createTempFile("spark-event", ".log")
  }

  override def afterEach() {
    eventLogFile.delete()
  }

  def withLocalSpark[T](f: SparkContext => T) =
    withSpark(new SparkContext("local", "test"))(f)

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
    System.setProperty("spark.eventLogging.enabled", "true")
    System.setProperty("spark.eventLogging.eventLogPath", eventLogFile.getAbsolutePath)

    withLocalSpark { sc =>
      implicit val actorSystem = sc.env.actorSystem
      val eventProcessed = Promise[Unit]()

      // Add a dummy listener to indicate that all listeners have been called
      sc.addSparkListener(new SparkListener {
        override def onRDDCreation(rddCreation: SparkListenerRDDCreation) {
          eventProcessed.success(())
        }
      })

      // Create a new RDD, a SparkListenerRDDCreation event should be written
      new EmptyRDD[Unit](sc)

      // Wait until the EventLogger is finally called
      assert(Await.result(eventProcessed.future, AwaitTimeout) === ())
    }

    val input = new ObjectInputStream(new FileInputStream(eventLogFile))

    try {
      val event = input.readObject().asInstanceOf[SparkListenerRDDCreation]
      assert(event.rdd.isInstanceOf[EmptyRDD[Unit]])
    }
    finally {
      input.close()
    }
  }
}

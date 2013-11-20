package org.apache.spark

import org.scalatest.FunSuite
import org.apache.spark.scheduler.{SparkListenerRDDCreation, SparkListener}
import akka.dispatch.{Await, Promise}
import akka.util.duration._
import org.apache.spark.rdd.EmptyRDD

class EventLoggingSuite extends FunSuite with LocalSparkContext {
  import LocalSparkContext.withSpark

  implicit val actorSystem = sc.env.actorSystem

  def withLocalSpark[T](f: SparkContext => T) =
    withSpark(new SparkContext("local", "test"))(f)

  test("A SparkListenerRDDCreation event should be posted when an RDD is created") {
    withLocalSpark { sc =>
      val eventPosted = Promise[Boolean]()

      sc.addSparkListener(new SparkListener {
        override def onRDDCreation(rddCreation: SparkListenerRDDCreation) {
          eventPosted.success(true)
        }
      })

      new EmptyRDD[Unit](sc)
      assert(Await.result(eventPosted.future, 1 second) === true)
    }
  }
}

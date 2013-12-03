package org.apache.spark.examples

import org.apache.spark._

/**
 * An example to show how to use `EventReplayer`
 */
object EventReplayerTest extends App {
  if (args.length < 1) {
    System.err.println("Usage: EventReplayerTest <master>")
    System.exit(1)
  }

  // Enables event logging
  System.setProperty("spark.eventLogging.enabled", "true")
  System.setProperty("spark.eventLogging.eventLogPath", "/tmp/replay.log")

  val sc = new SparkContext(args(0), "EventReplayerTest",
    System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))

  // Makes 2 RDDs
  val r0 = sc.makeRDD(1 to 4)
  val r1 = r0.map(_ * 2)

  // Runs the job. Events would be logged into /tmp/replay.log
  r1.collect()

  // Makes an `EventReplayer` which loads events from /tmp/replay.log
  val replayer = new EventReplayer(sc)

  // Lists all RDDs created in the job
  replayer.printRDDs()

  // Visualizes the 2 RDDs created earlier.
  replayer.visualizeRDDs("png", "rdds.png")

  try {
    // Adds an assertion to the reconstructed RDD and re-run the job.
    // Notice that this time the job would fail because of assertion error.
    val x1 = replayer.rdds(1)
    val x1WithAssertion = replayer.assertExists[Int](x1) { _ == 0 }
    x1WithAssertion.collect()
  } catch {
    case e: SparkException =>
      println(e)
  } finally {
    sc.stop()
  }
}

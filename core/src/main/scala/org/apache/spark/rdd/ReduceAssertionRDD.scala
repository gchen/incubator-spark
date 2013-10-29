package org.apache.spark.rdd

import org.apache.spark.util.~>
import org.apache.spark._
import scala.Some

class ReduceAssertionRDD[T: ClassManifest](
    prev: RDD[T],
    reducer: (T, T) => T,
    assertion: (T, Partition) => Option[AssertionFailure])
  extends RDD[T](prev.context, List(new OneToOneDependency(prev))) {

  def compute(split: Partition, context: TaskContext): Iterator[T] =
    new ReducingIterator(prev.iterator(split, context), split)

  protected def getPartitions: Array[Partition] = prev.partitions

  class ReducingIterator(underlying: Iterator[T], split: Partition) extends Iterator[T] {
    def hasNext: Boolean = underlying.hasNext

    def next(): T = {
      require(hasNext, new UnsupportedOperationException("no more elements"))

      val result = underlying.next()
      updateIntermediate(result)
      if (!hasNext) checkAssertion()
      result
    }

    private var intermediate: Option[T] = None

    private def updateIntermediate(element: T) {
      intermediate = intermediate match {
        case None => Some(element)
        case Some(x) => Some(reducer(x, element))
      }
    }

    private def checkAssertion() {
      for (x <- intermediate; failure <- assertion(x, split))
        SparkEnv.get.eventReporter.reportAssertionFailure(failure)
    }
  }

  override def mapDependencies(g: ~>[RDD, RDD]): RDD[T] =
    new ReduceAssertionRDD(g(firstParent), reducer, assertion)

  reportCreation()
}

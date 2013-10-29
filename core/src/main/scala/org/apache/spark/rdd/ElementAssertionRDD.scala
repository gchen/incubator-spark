package org.apache.spark.rdd

import org.apache.spark.util.~>
import org.apache.spark._

class ElementAssertionRDD[T: ClassManifest](
    prev: RDD[T],
    elementAssertion: (T, Partition) => Option[AssertionFailure])
  extends RDD[T](prev.context, List(new OneToOneDependency(prev))) {

  def compute(split: Partition, context: TaskContext): Iterator[T] = {
    prev.iterator(split, context).map { (element: T) =>
      for (failure <- elementAssertion(element, split))
        SparkEnv.get.eventReporter.reportAssertionFailure(failure)
      element
    }
  }

  override def mapDependencies(g: RDD ~> RDD): RDD[T] =
    new ElementAssertionRDD[T](g(firstParent), elementAssertion)

  protected def getPartitions: Array[Partition] = prev.partitions

  reportCreation()
}

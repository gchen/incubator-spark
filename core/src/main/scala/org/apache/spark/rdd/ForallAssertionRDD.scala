package org.apache.spark.rdd

import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.util.Utils.~>
import org.apache.spark.scheduler.SparkListenerAssertionFailure

class ForallAssertionRDD[T: ClassManifest](
    prev: RDD[T],
    assertion: (T, Partition) => Option[SparkListenerAssertionFailure])
  extends RDD[T](prev) { self =>

  def compute(split: Partition, context: TaskContext): Iterator[T] =
    prev.iterator(split, context).map { element =>
      assertion(element, split).foreach(self.context.postSparkListenerEvent)
      element
    }

  protected def getPartitions: Array[Partition] = prev.partitions

  override private[spark] def dependenciesUpdated(g: RDD ~> RDD) =
    new ForallAssertionRDD[T](g(firstParent), assertion)
}

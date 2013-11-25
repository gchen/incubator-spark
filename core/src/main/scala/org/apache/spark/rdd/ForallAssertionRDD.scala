package org.apache.spark.rdd

import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.util.Utils.~>

class ForallAssertionRDD[T: ClassManifest](
    prev: RDD[T],
    assertion: (T, Partition) => Unit)
  extends RDD[T](prev) { self =>

  def compute(split: Partition, context: TaskContext): Iterator[T] =
    prev.iterator(split, context).map { element =>
      assertion(element, split)
      element
    }

  protected def getPartitions: Array[Partition] = prev.partitions

  override private[spark] def dependenciesUpdated(g: RDD ~> RDD) =
    new ForallAssertionRDD[T](g(firstParent), assertion)
}

package org.apache.spark.shuffle.alluxio

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/**
  * Created by weijia.liu 
  * Date :  2016/10/26.
  * Time :  21:08
  */
class AlluxioSparkShuffleSorter extends AlluxioShuffleSorter with Logging{
  override def sort[K, C](context: TaskContext, keyOrd: Ordering[K], ser: Serializer,
                          alluxioDeserializationStream: AlluxioDeserializationStream): Iterator[Product2[K, C]] = {
    // use external sorter to implement sort
    val sorter = new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = ser)
    val input = alluxioDeserializationStream.asKeyValueIterator.asInstanceOf[Iterator[Product2[K, C]]]
    sorter.insertAll(input)
    context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
    context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
    context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
    CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
  }
}

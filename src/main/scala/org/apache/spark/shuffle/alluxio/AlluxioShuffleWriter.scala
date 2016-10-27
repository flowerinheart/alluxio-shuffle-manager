
package org.apache.spark.shuffle.alluxio

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleWriter}
import org.apache.spark.storage.AlluxioStore
import org.apache.spark.{SparkEnv, TaskContext}

/**
  * Created by weijia.liu
  * Date :  2016/10/25.
  * Time :  20:06
  */
private[spark] class AlluxioShuffleWriter[K, V](
                                                 shuffleBlockManager: AlluxioShuffleBlockResolver,
                                                 handle: BaseShuffleHandle[K, V, _],
                                                 mapId: Int,
                                                 context: TaskContext,
                                                 alluxioSerializer: AlluxioShuffleSerializer)
  extends ShuffleWriter[K, V] with Logging{

  private val dep = handle.dependency
  private val blockManager = SparkEnv.get.blockManager

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false
  private val writeMetrics = context.taskMetrics().shuffleWriteMetrics
  private val serialzerInstance = alluxioSerializer.newAlluxioSerializer(dep)
  private val writerGroup = AlluxioStore.get.getWriterGroup(dep.shuffleId, dep.partitioner.numPartitions, serialzerInstance, writeMetrics)

  /** Write a sequence of records to this task's output */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val iter = if (dep.aggregator.isDefined) {
      // need aggregate
      if (dep.mapSideCombine) {
        // need combine in map side (maybe write data to disk)
        dep.aggregator.get.combineValuesByKey(records, context)
      } else {
        // directly assign
        records
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      records
    }

    for (elem <- iter) {
      // TODO reduce call getPartition method
      val partitionId = dep.partitioner.getPartition(elem._1)
      writerGroup.getWriters(partitionId).write(elem._1, elem._2)
    }
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    if (!stopping) {
      logInfo("begin to stop writers")
      stopping = true
      if (success) {
        logInfo("success is true, begin to call writers' flush and release")
        writerGroup.flush()
        val sizes: Array[Long] = writerGroup.release()
        return Some(MapStatus(blockManager.shuffleServerId, sizes))
      }
    }
    None
  }
}
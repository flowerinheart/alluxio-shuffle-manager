
package org.apache.spark.shuffle.alluxio

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleWriter}
import org.apache.spark.storage.{AlluxioIndexFile, AlluxioStore, PartitionIndex}
import org.apache.spark.{SparkEnv, TaskContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
  * Created by weijia.liu
  * Date :  2016/10/25.
  * Time :  20:06
  */
private[spark] class AlluxioShuffleWriter[K, V](
                                                 shuffleBlockManager: AlluxioShuffleBlockResolver,
                                                 handle: BaseShuffleHandle[K, V, _],
                                                 mapId: Int,
                                                 context: TaskContext)
  extends ShuffleWriter[K, V] with Logging{

  private val dep = handle.dependency
  private val blockManager = SparkEnv.get.blockManager

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false
  private val writeMetrics = context.taskMetrics().shuffleWriteMetrics
  private val partitionLengths = new ArrayBuffer[Long]

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

    // TODO a task write the only one partition data to a file and mark offset in a index file
    // TODO maybe result in OOM
    val cacheMap = new mutable.HashMap[Int, ArrayBuffer[Product2[Any, Any]]]
    for (elem: Product2[Any, Any] <- iter) {
      val partitionId = dep.partitioner.getPartition(elem._1)
      val arrayBuffer = cacheMap.getOrElse(partitionId, new ArrayBuffer[Product2[Any, Any]])
      if (arrayBuffer.isEmpty) {
        cacheMap(partitionId) = arrayBuffer
      }
      arrayBuffer.append(elem)
    }

    // write data and index file, then close all streams
    val dataWriter = AlluxioStore.get.getDataWriter(dep.shuffleId, mapId, dep.serializer.newInstance(), writeMetrics)
    val indexWriter = AlluxioStore.get.getIndexWriter(dep.shuffleId, mapId)
    val indexFile = new AlluxioIndexFile
    var curLength: Long = 0
    for ((k, v) <- cacheMap) {
      val length = dataWriter.writeRecords(v)
      indexFile.addIndex(new PartitionIndex(k, curLength, length - curLength))
      partitionLengths.append(length - curLength)
      logInfo(s"partition $k length is ${length - curLength}")
      curLength = length
    }
    dataWriter.close()
    indexWriter.write(indexFile.getBytes)
    indexWriter.close()
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    if (!stopping) {
      stopping = true
      if (success) {
        return Some(MapStatus(blockManager.shuffleServerId, partitionLengths.toArray))
      }
    }
    None
  }
}

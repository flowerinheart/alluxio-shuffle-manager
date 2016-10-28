
package org.apache.spark.shuffle.alluxio

import java.util

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
  //private val serialzerInstance = alluxioSerializer.newAlluxioSerializer(dep)
  private val writerGroup = AlluxioStore.get.getWriterGroup(dep.shuffleId, dep.partitioner.numPartitions, dep.serializer.newInstance(), writeMetrics)
  private val recordsCache: util.HashMap[Int, util.List[Product2[K, Any]]] = new util.HashMap[Int, util.List[Product2[K, Any]]](dep.partitioner.numPartitions)

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

    while (iter.hasNext) {
      val elem = iter.next()
      val partitionId = dep.partitioner.getPartition(elem._1)
      var list = recordsCache.get(partitionId)
      if (list != null) {
        list.add(elem)
      } else {
        list = new util.ArrayList[Product2[K, Any]](){{add(elem)}}
        recordsCache.put(partitionId, list)
      }

      if (list.size()%10000 == 0) {
        batchWrite(list, partitionId)
      }
    }

    val mapIter = recordsCache.entrySet().iterator()
    while (mapIter.hasNext) {
      val entry = mapIter.next()
      batchWrite(entry.getValue, entry.getKey)
    }
  }

  private def batchWrite(list: util.List[Product2[K, Any]], partitionId: Int): Unit = {
    AlluxioShuffleWriter.lock.synchronized {
      val listIter = list.iterator()
      while (listIter.hasNext) {
        val elem = listIter.next()
        writerGroup.getWriters(partitionId).write(elem._1, elem._2.asInstanceOf[V])
      }
      list.clear()
    }
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    if (!stopping) {
      logInfo("begin to stop writers")
      stopping = true
      if (success) {
        logInfo("success is true, begin to call writers' release")
        val sizes: Array[Long] = writerGroup.release()
        return Some(MapStatus(blockManager.shuffleServerId, sizes))
      }
    }
    None
  }
}

object AlluxioShuffleWriter {
  val lock = new Object
}

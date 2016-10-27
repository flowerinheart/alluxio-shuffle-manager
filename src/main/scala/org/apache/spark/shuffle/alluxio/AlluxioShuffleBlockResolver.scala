
package org.apache.spark.shuffle.alluxio

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.shuffle.ShuffleBlockResolver
import org.apache.spark.storage.ShuffleBlockId

/**
  * Created by weijia.liu
  * Date :  2016/10/25.
  * Time :  20:06
  */
class AlluxioShuffleBlockResolver(conf: SparkConf) extends ShuffleBlockResolver with Logging{
  /**
    * Retrieve the data for the specified block. If the data for that block is not available,
    * throws an unspecified exception.
    */
  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    null
  }

  override def stop(): Unit = {

  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.shuffle.alluxio

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._
import org.apache.spark.storage.AlluxioStore

/**
  * Created by weijia.liu
  * Date :  2016/10/25.
  * Time :  20:06
  */
private[spark] class AlluxioShuffleManager(conf : SparkConf) extends ShuffleManager with Logging{

  logInfo("Use alluxio shuffle manager !!!")

  // not used yet
  private val fileShuffleBlockManager = new AlluxioShuffleBlockResolver(conf)

  /**
    * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
    */
  override def registerShuffle[K, V, C](shuffleId: Int, numMaps: Int, dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    AlluxioStore.get.registerShuffle(shuffleId, numMaps, dependency.partitioner.numPartitions)
    new BaseShuffleHandle[K, V, C](shuffleId, numMaps, dependency)
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V] = {
    new AlluxioShuffleWriter[K, V](shuffleBlockResolver, handle.asInstanceOf[BaseShuffleHandle[K, V, _]], mapId, context)
  }

  /**
    * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
    * Called on executors by reduce tasks.
    */
  override def getReader[K, C](handle: ShuffleHandle, startPartition: Int, endPartition: Int, context: TaskContext): ShuffleReader[K, C] = {
    new AlluxioShuffleReader[K, C](handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition, endPartition, context)
  }

  /**
    * Remove a shuffle's metadata from the ShuffleManager.
    *
    * @return true if the metadata removed successfully, otherwise false.
    */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    AlluxioStore.get.unregisterShuffle(shuffleId)
    true
  }

  /**
    * Return a resolver capable of retrieving shuffle block data based on block coordinates.
    */
  override def shuffleBlockResolver: AlluxioShuffleBlockResolver = {
    fileShuffleBlockManager
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    AlluxioStore.get.releaseAllShuffleData()
    AlluxioStore.put()
  }
}

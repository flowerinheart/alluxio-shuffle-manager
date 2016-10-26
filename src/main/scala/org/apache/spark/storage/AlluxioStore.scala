package org.apache.spark.storage

import alluxio.client.file.FileSystem

/**
  * Created by weijia.liu 
  * Date :  2016/10/25.
  * Time :  20:06
  */
class AlluxioStore {
  private val fs : FileSystem = FileSystem.Factory.get()
}



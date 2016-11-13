package org.apache.spark.storage

import java.nio.ByteBuffer

import org.apache.spark.internal.Logging

import scala.collection.mutable.ArrayBuffer

/**
  * Created by weijia.liu 
  * Date :  2016/11/7.
  * Time :  9:21
  */
/**
  * Alluxio Index File
  */
class AlluxioIndexFile{
  private var indexes = new ArrayBuffer[PartitionIndex]

  def this(indexArray: ArrayBuffer[PartitionIndex]) {
    this()
    indexes = indexArray
  }

  def addIndex(index: PartitionIndex) = {
    indexes.append(index)
  }

  def getBytes: Array[Byte] = {
    val byteBuffer = ByteBuffer.allocate(indexes.length*20)
    for (index <- indexes) {
      byteBuffer.putInt(index.partitionId)
      byteBuffer.putLong(index.startPos)
      byteBuffer.putLong(index.length)
    }
    byteBuffer.array()
  }

  def getPartitionIndexes: ArrayBuffer[PartitionIndex] = indexes
}

object AlluxioIndexFile extends Logging {
  def newInstance(bytes: Array[Byte]): AlluxioIndexFile = {
    val byteBuffer = ByteBuffer.wrap(bytes)
    val indexes = new ArrayBuffer[PartitionIndex]
    val loop = bytes.length / 20
    for (i <- 0 until loop) {
      indexes.append(new PartitionIndex(byteBuffer.getInt(), byteBuffer.getLong, byteBuffer.getLong()))
    }
    new AlluxioIndexFile(indexes)
  }
}

class PartitionIndex(val partitionId: Int, val startPos: Long, val length: Long) {

  override def toString: String = {
    new StringBuilder().append("partitionId : ").append(partitionId)
      .append(", startPos : ").append(startPos).append(", length : ").append(length).toString()
  }
}

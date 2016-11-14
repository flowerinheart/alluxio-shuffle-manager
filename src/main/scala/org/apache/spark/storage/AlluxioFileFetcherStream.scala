package org.apache.spark.storage

import java.io.InputStream

import alluxio.client.file.MultiBlockInStream
import org.apache.spark.internal.Logging

/**
  * Created by weijia.liu 
  * Date :  2016/11/7.
  * Time :  16:20
  */
class AlluxioFileFetcherStream(val multiBlockInStream: MultiBlockInStream)
  extends InputStream with Logging {

  private var readBytesSum = 0

  override def read(): Int = {
    multiBlockInStream.read()
  }

  override def read(b: Array[Byte]): Int = {
    val readBytes = multiBlockInStream.read(b)
    readBytesSum += (if (readBytes == -1) 0 else readBytes)
    readBytes
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    val readBytes = multiBlockInStream.read(b, off, len)
    readBytesSum += (if (readBytes == -1) 0 else readBytes)
    readBytes
  }

  override def close(): Unit ={
    multiBlockInStream.close()
  }

  override def skip(n: Long): Long = {
    multiBlockInStream.skip(n)
  }

  def getTotalReadBytes: Long = readBytesSum
}

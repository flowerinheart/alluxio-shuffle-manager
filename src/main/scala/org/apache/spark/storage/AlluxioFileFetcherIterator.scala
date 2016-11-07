package org.apache.spark.storage

import java.io.InputStream
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ArrayBlockingQueue, Executors, Future}

import com.esotericsoftware.kryo.io.ByteBufferInputStream
import org.apache.alluxio.api.streams.PartitionFileInStream
import org.apache.spark.internal.Logging

/**
  * Created by weijia.liu 
  * Date :  2016/11/7.
  * Time :  16:20
  */
class AlluxioFileFetcherIterator(val partitionFileInStreams: Array[PartitionFileInStream])
  extends Iterator[InputStream] with Logging {

  // every element's size is less than 4KB, then the queue is less than 64MB
  private val results = new ArrayBlockingQueue[Array[Byte]](10000)
  private val processedNum = new AtomicLong(0)
  private val processedSize = new AtomicLong(0)
  private val bufferSize = 8*1024*1024

  initialize()

  private def initialize(): Unit = {
    AlluxioFileFetcherIterator.submit(new Task)
  }

  override def hasNext: Boolean = processedNum.get() < partitionFileInStreams.length

  override def next(): InputStream = {
    val byteBuffer = ByteBuffer.allocate(bufferSize)
    var size: Long = 0
    while (size < bufferSize) {
      val curResult = results.take()
      byteBuffer.put(curResult)
      size += curResult.length
      if (processedNum.incrementAndGet() >= partitionFileInStreams.length || results.isEmpty) {
        return new ByteBufferInputStream(byteBuffer)
      }
    }
    new ByteBufferInputStream(byteBuffer)
  }

  def cleanUp(): Unit = {

  }

  class Task extends Runnable {

    private var sum: Long = 0

    override def run(): Unit = {
      for (partitionFile <- partitionFileInStreams) {
        val begin = System.nanoTime()
        val data = new Array[Byte](partitionFile.getLength.asInstanceOf[Int])
        if (partitionFile.read(data) != -1) {
          partitionFile.close()
          results.put(data)
          processedSize.addAndGet(partitionFile.getLength)
        } else {
          logError(s"read data return -1, partition file is ${partitionFile.toString}")
        }
        val end = System.nanoTime()
        logInfo(s"fetch partition spent ${(end - begin)/1000000} ms")
        sum += (end - begin)
      }
      logInfo(s"fetch ${partitionFileInStreams.length} partitions total spent ${sum/1000000} ms")
    }
  }
}

object AlluxioFileFetcherIterator {
  private val threadPool = Executors.newFixedThreadPool(5)

  def submit(task: Runnable): Future[_] = {
    threadPool.submit(task)
  }

  def shutdown(): Unit = {
    threadPool.shutdown()
  }
}

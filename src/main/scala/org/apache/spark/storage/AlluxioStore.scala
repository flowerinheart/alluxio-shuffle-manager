package org.apache.spark.storage

import java.io.OutputStream
import java.net.InetAddress
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import alluxio.client.file.options.{CreateDirectoryOptions, CreateFileOptions, DeleteOptions, OpenFileOptions}
import alluxio.client.file.{FileInStream, FileOutStream, FileSystem}
import alluxio.client.{ClientContext, ReadType, WriteType}
import alluxio.{AlluxioURI, Configuration, PropertyKey}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.{SparkConf, SparkEnv}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by weijia.liu 
  * Date :  2016/10/25.
  * Time :  20:06
  */
class AlluxioStore extends Logging {
  // alluxio file system client
  private var fs: FileSystem = _

  // members init form spark env
  val executorId: String = SparkEnv.get.executorId
  val sparkConf: SparkConf = SparkEnv.get.conf
  val appId: String = sparkConf.getAppId
  val serializer: Serializer = SparkEnv.get.serializer

  val rootDir = "/spark"
  val shuffleDir = rootDir + "/shuffle"
  var hostname: String = InetAddress.getLocalHost.getHostName
  var alluxioBlockSize: Long = 0
  var alluxioMemOnly: Boolean = false
  var alluxioReadWithoutCache: Boolean = false

  // key : shuffleId, value : fileId
  val shuffleWriterGroups = new mutable.HashMap[Int, AlluxioShuffleWriterGroup]
  val unregLock = new Object()
  val getWritersLock = new Object()

  /**
    * init AlluxioStore
    */
  def init(): Unit = {
    logInfo("begin to init Alluxio Store.")

    // init alluxio filesystem client
    alluxioBlockSize = sparkConf.getLong("spark.alluxio.block.size", 536870912)
    alluxioMemOnly = sparkConf.getBoolean("spark.alluxio.memory.only", defaultValue = true)
    alluxioReadWithoutCache = sparkConf.getBoolean("spark.alluxio.read.without.cache", defaultValue = true)
    val alluxioMasterHost = sparkConf.get("spark.alluxio.master.host")
    if (alluxioMasterHost == null) {
      throw new Exception("get spark.alluxio.master.host return null")
    } else {
      val masterLocation: AlluxioURI = new AlluxioURI(alluxioMasterHost)
      Configuration.set(PropertyKey.MASTER_HOSTNAME, masterLocation.getHost)
      Configuration.set(PropertyKey.MASTER_RPC_PORT, Integer.toString(masterLocation.getPort))
      ClientContext.init()
      fs = FileSystem.Factory.get()
    }

    logInfo(s"alluxio master host is $alluxioMasterHost")
    logInfo(s"alluxio block size is $alluxioBlockSize")
    logInfo(s"alluxio use memory only flag is $alluxioMemOnly")
    logInfo(s"alluxio read without cache flag is $alluxioReadWithoutCache")

    if (executorId.equals("driver")) {
      // driver executor create root dir
      val shuffleUri: AlluxioURI = new AlluxioURI(shuffleDir)
      if (!fs.exists(shuffleUri)) {
        logInfo(s"$shuffleDir is not exist, need to create.")
        val options: CreateDirectoryOptions = CreateDirectoryOptions.defaults().setAllowExists(true).setRecursive(true)
        // don't catch exception
        fs.createDirectory(shuffleUri, options)
        logInfo(s"create $shuffleDir success.")
      } else {
        logInfo(s"$shuffleDir is exist, don't need to create again.")
      }
    } else {
      logInfo(s"this is not diver executor, don't need to create $shuffleDir")
    }

    logInfo("init Alluxio Store over.")
  }

  /**
    * shutdown AlluxioStore
    */
  def shutdown(): Unit = {
    logInfo("begin to shutdown Alluxio Store.")
    // FileSystem client has not shutdown or close or stop method
    fs = null
    logInfo("shutdown Alluxio Store over.")
  }

  /**
    * register shuffle and create directories on Alluxio
    * @param shuffleId shuffle id
    * @param numMaps number of maps
    * @param partitions rdd partition nums
    */
  def registerShuffle(shuffleId: Int, numMaps: Int, partitions: Int): Unit = {
    logInfo(s"begin to register shuffle, shuffle id is $shuffleId .")
    if (shuffleExists(shuffleId)) {
      logInfo(s"shuffle id is : $shuffleId, this shuffle has registered")
    } else {
      val shuffleIdDir = shuffleDir + "/" + appId + "/shuffle_" + shuffleId
      val options: CreateDirectoryOptions = CreateDirectoryOptions.defaults().setAllowExists(true).setRecursive(true)
      for (i <- 0 until partitions) {
        val partDir = shuffleIdDir + "/part_" + i
        fs.createDirectory(new AlluxioURI(partDir), options)
        logInfo(s"create $partDir successful on Alluxio.")
      }
      logInfo(s"register shuffle over, shuffle id is $shuffleId .")
    }
  }

  /**
    * unregister shuffle and delete directories from Alluxio
    * @param shuffleId shuffle id
    */
  def unregisterShuffle(shuffleId: Int): Unit = {
    logInfo(s"begin to unregister shuffle, shuffle id is $shuffleId")
    try {
      if (shuffleId > 0 && shuffleExists(shuffleId)) {
        // when shuffle id is exist, delete shuffle id dir from alluxio
        deleteAlluxioFiles(shuffleId)
        shuffleWriterGroups -= shuffleId
      }
    } catch {
      case e: Exception => logInfo(s"failed to unregister shuffle, shuffle id is $shuffleId")
    }
    logInfo(s"unregister shuffle over, shuffle id is $shuffleId")
  }

  /**
    * direct delete the application directory from Alluxio
    */
  def releaseAllShuffleData(): Unit = {
    for (key <- shuffleWriterGroups.keySet) {
      deleteAlluxioFiles(key)
      shuffleWriterGroups -= key
    }
    fs.delete(new AlluxioURI(shuffleDir + "/" + appId), DeleteOptions.defaults().setRecursive(true))
  }

  /**
    * release the shuffle writer group
    * @param shuffleId shuffle id
    */
  def releaseShuffleWriterGroup(shuffleId: Int): Unit = {
    shuffleWriterGroups(shuffleId).release()
  }

  /**
    * delete the shuffle directory
    * @param shuffleId shuffle id
    */
  private def deleteAlluxioFiles(shuffleId: Int): Unit = {
    val options: DeleteOptions = DeleteOptions.defaults().setRecursive(true)
    val shuffleIdDir = new AlluxioURI(shuffleDir + "/" + appId + "/shuffle_" + shuffleId)
    // ensure thread safe
    unregLock.synchronized {
      if (fs.exists(shuffleIdDir)) {
        fs.delete(shuffleIdDir, options)
      }
    }
    logInfo(s"delete $shuffleIdDir successfully from Alluxio.")
  }

  /**
    * get shuffle writer group, this method would create files on Alluxio and get FileOutStreams
    * @param shuffleId shuffle id
    * @param numPartitions number of partitions
    * @param serializerInstance serializer instance, serialize record's key and value
    * @param writeMetrics shuffle write metrics, statistic records number have been written
    * @return return an AlluxioShuffleWriterGroup object
    */
  def getWriterGroup(shuffleId: Int, numPartitions: Int,
                     serializerInstance: SerializerInstance,
                     writeMetrics: ShuffleWriteMetrics): AlluxioShuffleWriterGroup = {
    if (!shuffleWriterGroups.contains(shuffleId)) {
      getWritersLock.synchronized {
        if (!shuffleWriterGroups.contains(shuffleId)) {
          // every shuffle dependency corresponds to one writer group, and one writer corresponds to one partition
          val streams = new Array[FileOutStream](numPartitions)
          for (i <- 0 until numPartitions) {
            val sb: StringBuilder = new StringBuilder()
            sb.append(shuffleDir).append("/").append(appId).append("/shuffle_").append(shuffleId).append("/part_").append(i)
              .append("/").append(executorId).append("_").append(hostname)
            val options: CreateFileOptions = CreateFileOptions.defaults()
              .setWriteType(if (alluxioMemOnly) WriteType.MUST_CACHE else WriteType.CACHE_THROUGH)
              .setBlockSizeBytes(alluxioBlockSize).setRecursive(true)
            streams(i) = fs.createFile(new AlluxioURI(sb.toString()), options)
          }
          shuffleWriterGroups.put(shuffleId, new AlluxioShuffleWriterGroup(streams, shuffleId, serializerInstance, writeMetrics))
        }
      }
    }

    shuffleWriterGroups(shuffleId)
  }

  /**
    * open files and return FileInStreams on Alluxio
    * @param shuffleId shuffle id
    * @param startPrtId start partition id
    * @param endPartId end partition id
    * @return an array of FileInStream
    */
  def getFileInStreams(shuffleId: Int, startPrtId: Int, endPartId: Int): Array[FileInStream] = {
    logInfo(s"start partition is $startPrtId, end partition is $endPartId")
    val options = OpenFileOptions.defaults().setReadType(if (alluxioReadWithoutCache) ReadType.NO_CACHE else ReadType.CACHE_PROMOTE)
    val streams = ArrayBuffer.empty[FileInStream]
    for (i <- startPrtId until endPartId) {
      // to make sure thread safe, every executor only read the files which id equals its executor id
      val sb: StringBuilder = new StringBuilder()
      sb.append(shuffleDir).append("/").append(appId).append("/shuffle_").append(shuffleId).append("/part_").append(i).append("/")
      val fileStatus = fs.listStatus(new AlluxioURI(sb.toString()))
      val iter = fileStatus.iterator()
      while (iter.hasNext) {
        val status = iter.next()
        val fileUri = new AlluxioURI(status.getPath)
        if (checkFileStatus(status.getLength, fileUri)) {
          streams.append(fs.openFile(fileUri, options))
        }
      }
    }
    logInfo("get streams from alluxio size is " + streams.length)
    streams.toArray
  }

  /**
    * check whether the file is closed by judging the length of the file
    * @param length the file length
    * @param fileUri Alluxio uri
    * @return return true if the file is closed, otherwise false
    */
  private def checkFileStatus(length: Long, fileUri: AlluxioURI): Boolean = {
    if (length == 0) {
      for (i <- 0 to 5) {
        logWarning(s"${fileUri.getPath} length is 0, maybe hasn't been close, wait 100ms")
        Thread.sleep(100)
        val status = fs.getStatus(fileUri)
        if (status.getLength != 0) {
          return true
        }
      }
      false
    } else {
      true
    }
  }

  private def shuffleExists(shuffleId: Int): Boolean = {
    shuffleWriterGroups.contains(shuffleId)
  }
}

object AlluxioStore extends Logging {
  private val lock = new ReentrantLock()
  private var instance: AlluxioStore = _
  private val shutdown: AtomicBoolean = new AtomicBoolean(false)

  /**
    * get and init an AlluxioStore object
    * @return AlluxioStore instance
    */
  def get: AlluxioStore = {
    if (instance == null) {
      lock.lock()
      try {
        if (instance == null && !shutdown.get()) {
          val _instance = new AlluxioStore()
          _instance.init()
          instance = _instance
        } else if (instance == null && shutdown.get()) {
          throw new Exception("Error: Attempt to re-initialize Alluxio Store (!)")
        }
      } finally {
        lock.unlock()
      }
    }
    instance
  }

  /**
    * shutdown AlluxioStore and set shutdown flag
    */
  def put(): Unit = {
    if (instance != null && !shutdown.get()) {
      lock.lock()
      try {
        if (instance != null && !shutdown.get()) {
          instance.shutdown()
          shutdown.set(true)
          instance = null
        }
      } finally {
        lock.unlock()
      }
    }
  }
}

/**
  * define the AlluxioObjectWriter class
  * @param directStream alluxio file out stream
  * @param serializerInstance alluxio serializer instance
  * @param writeMetrics metrics tool
  * @param shuffleId shuffle id
  * @param reduceId reduce id
  */
private[spark] class AlluxioObjectWriter(directStream: FileOutStream, serializerInstance: SerializerInstance,
                                         writeMetrics: ShuffleWriteMetrics, shuffleId: Int, reduceId: Int)
  extends OutputStream with Logging {

  private var initialized = false
  private var hasBeenClosed = false
  // default initialization
  private var directObjOut: SerializationStream = _
  private var ts: TimeTrackingOutputStream = _
  private var finalPosition: Long = -1
  private var reportedPosition: Long = 0
  private val lock = new Object()

  /**
    * Keep track of number of records written and also use this to periodically
    * output bytes written since the latter is expensive to do for each record.
    */
  private var numRecordsWritten = 0

  /**
    * construct an AlluxioObjectWriter
    * @return AlluxioObjectWriter
    */
  def open(): AlluxioObjectWriter = {
    if (hasBeenClosed) {
      throw new IllegalStateException("Writer already closed. Cannot be reopened.")
    }
    ts = new TimeTrackingOutputStream(writeMetrics, directStream)
    directObjOut = serializerInstance.serializeStream(ts)
    initialized = true
    this
  }

  /**
    * close and release resource
    */
  override def close(): Unit = {
    if (initialized) {
      initialized = false
      finalPosition = length
      directObjOut.close()
      directStream.close()
      hasBeenClosed = false

      directObjOut = null
      ts = null
    } else {
      finalPosition = 0
    }
  }

  def isOpen: Boolean = initialized

  def length : Long = directStream.getBytesWritten

  override def write(b: Int): Unit = throw new UnsupportedOperationException()

  /**
    * FileOutStream is wrapped with TimeTrackingOutputStream to record spend time
    * @param b byte array
    * @param off offset
    * @param len length
    */
  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    if (!initialized) {
      open()
    }

    ts.write(b, off, len)
  }

  /**
    * write record's key and value with the serialized stream and write record num simultaneously
    * @param key record's key
    * @param value record's value
    */
  def write(key: Any, value: Any): Unit = {
    lock.synchronized {
      if (!initialized) {
        open()
      }

      directObjOut.writeKey(key)
      directObjOut.writeValue(value)
    }
    recordWritten()
  }

  /**
    * Notify the writer that a record worth of bytes has been written with OutputStream#write.
    */
  private def recordWritten(): Unit = {
    numRecordsWritten += 1
    writeMetrics.incRecordsWritten(1)

    if (numRecordsWritten % 16384 == 0) {
      updateBytesWritten()
      //flush()
    }
  }

  /**
    * Report the number of bytes written in this writer's shuffle write metrics.
    * Note that this is only valid before the underlying streams are closed.
    */
  private def updateBytesWritten() {
    writeMetrics.incBytesWritten(length - reportedPosition)
    reportedPosition = length
  }

  private[spark] override def flush(): Unit = {
    if (initialized) {
      directObjOut.flush()
      directStream.flush()
    }
  }
}

class AlluxioShuffleWriterGroup(val streams: Array[FileOutStream], shuffleId: Int,
                                serializerInstance: SerializerInstance,
                                writeMetrics: ShuffleWriteMetrics,
                                @volatile var isReleased: Boolean = false) extends Logging{
  private val writers: Array[AlluxioObjectWriter] = new Array[AlluxioObjectWriter](streams.length)
  private val releaseLock = new Object()

  logInfo(s"shuffle id is : $shuffleId, writer num is : " + writers.length)
  // init writers
  for (i <- streams.indices) {
    writers(i) = new AlluxioObjectWriter(streams(i), serializerInstance, writeMetrics, shuffleId, i)
  }

  def lengths(): Array[Long] = {
      writers.map{ writer => writer.length}
  }

  def release(): Unit = {
    if (!isReleased) {
      releaseLock.synchronized {
        if (!isReleased) {
          isReleased = true
          writers.foreach{ writer => writer.close()}
        }
      }
    }
  }

  def getWriters: Array[AlluxioObjectWriter] = writers
}





package org.apache.spark.storage

import java.io.OutputStream
import java.net.InetAddress
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import alluxio.client.file.options.{CreateDirectoryOptions, CreateFileOptions, DeleteOptions, OpenFileOptions}
import alluxio.client.file.{FileOutStream, FileSystem, URIStatus}
import alluxio.client.{ClientContext, ReadType, WriteType}
import alluxio.{AlluxioURI, Configuration, PropertyKey}
import org.apache.alluxio.api.streams.PartitionFileInStream
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.{SparkConf, SparkEnv}

import scala.collection.JavaConversions.asScalaBuffer
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

  val shuffleIds = new mutable.HashSet[Int]
  val partitionIndexes = new mutable.HashMap[Int, ArrayBuffer[(String, PartitionIndex)]]

  /**
    * init AlluxioStore
    */
  def init(): Unit = {
    logInfo("begin to init Alluxio Store.")

    // init alluxio filesystem client
    alluxioBlockSize = sparkConf.getLong("spark.alluxio.block.size", 536870912)
    alluxioMemOnly = sparkConf.getBoolean("spark.alluxio.memory.only", defaultValue = true)
    alluxioReadWithoutCache = sparkConf.getBoolean("spark.alluxio.read.without.cache", defaultValue = true)
    val alluxioRemoteReadBufferSize = sparkConf.get("spark.alluxio.remote.read.buffer.size", "1MB")
    val alluxioCachePartiallyReadBlock = sparkConf.getBoolean("spark.alluxio.cache.partially.read.block", defaultValue = false)
    val alluxioFileSeekBufferSize = sparkConf.get("spark.alluxio.file.seek.buffer.size", "1KB")
    val alluxioMasterHost = sparkConf.get("spark.alluxio.master.host")
    if (alluxioMasterHost == null) {
      throw new Exception("get spark.alluxio.master.host return null")
    } else {
      val masterLocation: AlluxioURI = new AlluxioURI(alluxioMasterHost)
      Configuration.set(PropertyKey.MASTER_HOSTNAME, masterLocation.getHost)
      Configuration.set(PropertyKey.MASTER_RPC_PORT, Integer.toString(masterLocation.getPort))
      Configuration.set(PropertyKey.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES, alluxioRemoteReadBufferSize)
      Configuration.set(PropertyKey.USER_FILE_CACHE_PARTIALLY_READ_BLOCK, alluxioCachePartiallyReadBlock)
      Configuration.set(PropertyKey.USER_FILE_SEEK_BUFFER_SIZE_BYTES, alluxioFileSeekBufferSize)
      ClientContext.init()
      fs = FileSystem.Factory.get()
    }

    logInfo(s"alluxio master host is $alluxioMasterHost")
    logInfo(s"alluxio block size is $alluxioBlockSize")
    logInfo(s"alluxio use memory only flag is $alluxioMemOnly")
    logInfo(s"alluxio read without cache flag is $alluxioReadWithoutCache")
    logInfo(s"alluxio remote read buffer size is $alluxioRemoteReadBufferSize")
    logInfo(s"alluxio cache partially read block is $alluxioCachePartiallyReadBlock")

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
      val shuffleDataDir = new StringBuilder().append(shuffleDir).append("/").append(appId)
        .append("/shuffle_").append(shuffleId).append("/data").toString()
      val shuffleIndexDir = new StringBuilder().append(shuffleDir).append("/").append(appId)
        .append("/shuffle_").append(shuffleId).append("/index").toString()
      val options: CreateDirectoryOptions = CreateDirectoryOptions.defaults().setAllowExists(true).setRecursive(true)
      fs.createDirectory(new AlluxioURI(shuffleDataDir), options)
      fs.createDirectory(new AlluxioURI(shuffleIndexDir), options)
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
        shuffleIds -= shuffleId
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
    for (shuffleId <- shuffleIds) {
      deleteAlluxioFiles(shuffleId)
      shuffleIds -= shuffleId
    }
    fs.delete(new AlluxioURI(shuffleDir + "/" + appId), DeleteOptions.defaults().setRecursive(true))
  }

  /**
    * delete the shuffle directory
    * @param shuffleId shuffle id
    */
  private def deleteAlluxioFiles(shuffleId: Int): Unit = {
    val options: DeleteOptions = DeleteOptions.defaults().setRecursive(true)
    val shuffleIdDir = new AlluxioURI(shuffleDir + "/" + appId + "/shuffle_" + shuffleId)
    // ensure thread safe
    synchronized {
      if (fs.exists(shuffleIdDir)) {
        fs.delete(shuffleIdDir, options)
      }
    }
    logInfo(s"delete $shuffleIdDir successfully from Alluxio.")
  }

  /**
    * get data file writer
    * @param shuffleId shuffle id
    * @param mapId map task id
    * @param serializerInstance serializer instance
    * @param writeMetrics metrics
    * @return AlluxioObjectWriter object
    */
  def getDataWriter(shuffleId: Int, mapId: Int, serializerInstance: SerializerInstance,
                writeMetrics: ShuffleWriteMetrics): AlluxioObjectWriter = {
    val filePath = new StringBuilder().append(shuffleDir).append("/").append(appId).append("/shuffle_").append(shuffleId)
      .append("/data/map_").append(mapId).append(".data").toString()
    val options: CreateFileOptions = CreateFileOptions.defaults()
      .setWriteType(if (alluxioMemOnly) WriteType.MUST_CACHE else WriteType.CACHE_THROUGH)
      .setBlockSizeBytes(alluxioBlockSize).setRecursive(true)
    val fos = fs.createFile(new AlluxioURI(filePath), options)
    new AlluxioObjectWriter(fos, serializerInstance,writeMetrics, shuffleId, mapId)
  }

  /**
    * get index file writer
    * @param shuffleId shuffle id
    * @param mapId map task id
    * @return FileOutStream object
    */
  def getIndexWriter(shuffleId: Int, mapId: Int): FileOutStream = {
    val filePath = new StringBuilder().append(shuffleDir).append("/").append(appId).append("/shuffle_").append(shuffleId)
      .append("/index/map_").append(mapId).append(".index").toString()
    val options: CreateFileOptions = CreateFileOptions.defaults()
      .setWriteType(if (alluxioMemOnly) WriteType.MUST_CACHE else WriteType.CACHE_THROUGH)
      .setBlockSizeBytes(alluxioBlockSize).setRecursive(true)
    fs.createFile(new AlluxioURI(filePath), options)
  }

  /**
    * open files and return FileInStreams on Alluxio
    * @param shuffleId shuffle id
    * @param startPrtId start partition id
    * @param endPartId end partition id
    * @return an array of FileInStream
    */
  def getFileInStreams(shuffleId: Int, startPrtId: Int, endPartId: Int): Array[PartitionFileInStream] = {
    val options = OpenFileOptions.defaults().setReadType(if (alluxioReadWithoutCache) ReadType.NO_CACHE else ReadType.CACHE_PROMOTE)
    checkAndUpdatePartitionIndexes(shuffleId, options)

    logInfo(s"start partition is $startPrtId, end partition is $endPartId")
    val streams = new ArrayBuffer[PartitionFileInStream]
    for (i <- startPrtId until endPartId) {
      val partitionIndexArray = partitionIndexes(i)
      for ((filePath, index) <- partitionIndexArray) {
        streams.append(new PartitionFileInStream(fs, filePath, options, index.startPos, index.length))
        //logInfo(s"$filePath start pos is ${index.startPos}, length is ${index.length}")
      }
    }
    logInfo("get streams from alluxio size is " + streams.length)
    streams.toArray
  }

  /**
    * check and update partition indexes
    * @param shuffleId shuffle id
    * @param options open file options
    */
  private def checkAndUpdatePartitionIndexes(shuffleId: Int, options: OpenFileOptions): Unit = {
    if (partitionIndexes.isEmpty) {
      synchronized {
        if (partitionIndexes.isEmpty) {
          // get all index files
          val indexDir = new StringBuilder().append(shuffleDir).append("/").append(appId)
            .append("/shuffle_").append(shuffleId).append("/index").toString()
          val fileStatus: mutable.Buffer[URIStatus] = fs.listStatus(new AlluxioURI(indexDir))
          for (status <- fileStatus) {
            val fis = fs.openFile(new AlluxioURI(status.getPath), options)
            val data = new Array[Byte](status.getLength.asInstanceOf[Int])
            if (fis.read(data) != -1) {
              // read data and convert to map
              val indexFile = AlluxioIndexFile.newInstance(data)
              for (partitionIndex <- indexFile.getPartitionIndexes) {
                val partitions = partitionIndexes.getOrElse(partitionIndex.partitionId, new ArrayBuffer[(String, PartitionIndex)])
                if (partitions.isEmpty) {
                  partitionIndexes(partitionIndex.partitionId) = partitions
                }
                partitions.append((status.getPath.replace("index", "data"), partitionIndex))
              }
            } else {
              logError(s"read ${status.getPath} file data return -1")
            }
          }
        }
      }
    }
  }

  private def shuffleExists(shuffleId: Int): Boolean = {
    shuffleIds.contains(shuffleId)
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

  def writeRecords(arrayBuffer: ArrayBuffer[Product2[Any, Any]]): Long = {
    for (elem <- arrayBuffer) {
      write(elem._1, elem._2)
    }
    // ensure return the right length
    flush()
    length
  }

  /**
    * write record's key and value with the serialized stream and write record num simultaneously
    * @param key record's key
    * @param value record's value
    */
  def write(key: Any, value: Any): Unit = {
    if (!initialized) {
      open()
    }

    directObjOut.writeKey(key)
    directObjOut.writeValue(value)
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




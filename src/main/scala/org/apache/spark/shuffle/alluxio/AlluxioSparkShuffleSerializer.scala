package org.apache.spark.shuffle.alluxio
import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer

import alluxio.client.file.{FileInStream, FileOutStream}
import org.apache.spark.ShuffleDependency
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, SerializerInstance}

import scala.reflect.ClassTag

/**
  * Created by weijia.liu 
  * Date :  2016/10/26.
  * Time :  10:55
  */
class AlluxioSparkShuffleSerializer extends AlluxioShuffleSerializer {
  override def newAlluxioSerializer[K, V](dep: ShuffleDependency[K, V, _]): AlluxioSerializerInstance = {
    new AlluxioSparkShuffleSerializerInstance(dep.serializer.newInstance())
  }
}

class AlluxioSparkShuffleSerializerInstance(val serializerInstance: SerializerInstance) extends AlluxioSerializerInstance {
  override def serializeAlluxioStream(stream: OutputStream): AlluxioSerializationStream = {
    new AlluxioSparkSerializationStream(serializerInstance.serializeStream(stream))
  }

  override def deserializeAlluxioStream(stream: InputStream): AlluxioDeserializationStream = {
    new AlluxioSparkDeserializationStream(serializerInstance.deserializeStream(stream))
  }
}

class AlluxioSparkSerializationStream(serializationStream: SerializationStream) extends AlluxioSerializationStream {
  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    serializationStream.writeObject(t)
  }

  override final def writeKey[T: ClassTag](key: T): SerializationStream = {
    serializationStream.writeKey(key)
  }
  /** Writes the object representing the value of a key-value pair. */
  override final def writeValue[T: ClassTag](value: T): SerializationStream = {
    serializationStream.writeValue(value)
  }

  override final def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
    serializationStream.writeAll(iter)
  }

  override def flush(): Unit = {
    serializationStream.flush()
  }

  override def close(): Unit = {
    serializationStream.close()
  }
}

class AlluxioSparkDeserializationStream(deserializationStream: DeserializationStream) extends AlluxioDeserializationStream {
  override def getFlatBuffer(): ByteBuffer = {
    null
  }

  override def keySize(): Int = 0

  override def valueSize(): Int = 0

  override def numElems(): Int = 0

  override def readObject[T: ClassTag](): T = {
    deserializationStream.readObject()
  }

  /* for key we return the address */
  override final def readKey[T: ClassTag](): T = {
    deserializationStream.readKey()
  }

  override final def readValue[T: ClassTag](): T = {
    deserializationStream.readValue()
  }

  override def close(): Unit = {
    deserializationStream.close()
  }
}

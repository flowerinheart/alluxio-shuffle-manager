package org.apache.spark.shuffle.alluxio

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer

import org.apache.spark.ShuffleDependency
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.serializer.{DeserializationStream, SerializationStream}


/**
  * Created by weijia.liu 
  * Date :  2016/10/26.
  * Time :  10:36
  */
trait AlluxioShuffleSerializer {
  def newAlluxioSerializer[K, V](dep: ShuffleDependency[K, _, V]): AlluxioSerializerInstance
}

trait AlluxioSerializerInstance {
  def serializeAlluxioStream(stream: OutputStream): AlluxioSerializationStream

  def deserializeAlluxioStream(stream: InputStream): AlluxioDeserializationStream
}

@DeveloperApi
abstract class AlluxioSerializationStream extends SerializationStream {

}

@DeveloperApi
abstract class AlluxioDeserializationStream extends DeserializationStream {
  def getFlatBuffer(): ByteBuffer
  def keySize(): Int
  def valueSize(): Int
  def numElems(): Int
}

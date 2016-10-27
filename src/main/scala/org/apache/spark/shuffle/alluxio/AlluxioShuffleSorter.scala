package org.apache.spark.shuffle.alluxio

import org.apache.spark.TaskContext
import org.apache.spark.serializer.Serializer

/**
  * Created by weijia.liu 
  * Date :  2016/10/26.
  * Time :  21:03
  */
trait AlluxioShuffleSorter {
  def sort[K, C](context: TaskContext, keyOrd: Ordering[K], ser : Serializer,
                 alluxioDeserializationStream: AlluxioDeserializationStream): Iterator[Product2[K, C]]
}

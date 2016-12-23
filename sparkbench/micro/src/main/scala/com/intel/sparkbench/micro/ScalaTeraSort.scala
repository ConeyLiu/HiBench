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

package com.intel.hibench.sparkbench.micro


import com.intel.hibench.sparkbench.common.IOCommon
import com.intel.sparkbench.micro.{TeraInputFormat, TeraOutputFormat}
import org.apache.hadoop.io.{Text, WritableComparator}
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object ScalaTeraSort {
  implicit def rddToSampledOrderedRDDFunctions[K: Ordering : ClassTag, V: ClassTag]
  (rdd: RDD[(K, V)]) = new ConfigurableOrderedRDDFunctions[K, V, (K, V)](rdd)

  implicit def ArrayByteOrdering: Ordering[Array[Byte]] = Ordering.fromLessThan {
    case (a, b) => innerCompare(a, b) < 0
  }

  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println(
        s"Usage: $ScalaTeraSort <INPUT_HDFS> <OUTPUT_HDFS>"
      )
      System.exit(1)
    }
    val conf = new SparkConf()
         .setAppName("ScalaTeraSort")
         .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val data = sc.hadoopFile[Array[Byte], Array[Byte], TeraInputFormat](args(0))
      // If not clone, all keys are the same
      .map(kv => (kv._1.clone(), kv._2.clone()))

    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)
    val reducer  = IOCommon.getProperty("hibench.default.shuffle.parallelism")
      .getOrElse((parallel / 2).toString).toInt

    val partitioner = new BaseRangePartitioner(partitions = reducer, rdd = data)

    val partitioned_sorted_rdd = data.map(kv => (partitioner.getPartition(kv._1), (kv._1, kv._2)))
      .mapPartitions { iterator =>
        val hashmap = new mutable.HashMap[Int, ArrayBuffer[(Array[Byte], Array[Byte])]]()
        iterator.foreach { case (pid, kv) =>
            val arr = hashmap.getOrElseUpdate(pid, new ArrayBuffer[(Array[Byte], Array[Byte])]())
            arr += kv
        }

        hashmap.map{ case (pid, kv) =>
          kv.sortWith((r1, r2) => innerCompare(r1._1, r2._1) < 0)
          (pid, kv)
        }

        val hashMapIterator = hashmap.iterator

        assert(hashMapIterator.hasNext, "HashMap is empty.")

        val kvIterator = new Iterator[(Array[Byte], Array[Byte])] {

          var mapRecord = hashMapIterator.next()
          var mapRecordKey = mapRecord._1
          var mapRecordValueIter = mapRecord._2.toIterator

          override def hasNext: Boolean = hashMapIterator.hasNext || mapRecordValueIter.hasNext

          override def next(): (Array[Byte], Array[Byte]) = {
            if (mapRecordValueIter.hasNext) {
              mapRecordValueIter.next()
            } else {
              mapRecord = hashMapIterator.next()
              mapRecordKey = mapRecord._1
              mapRecordValueIter = mapRecord._2.toIterator
              mapRecordValueIter.next()
            }
          }
        }

        kvIterator
      }

    partitioned_sorted_rdd.sortByKeyWithPartitioner(partitioner)
        .saveAsHadoopFile[TeraOutputFormat](args(1))

    sc.stop()
  }

  def innerCompare(a: Array[Byte], b: Array[Byte]): Int = {
    if (a eq b) return 0
    WritableComparator.compareBytes(a, 0, a.length, b, 0, b.length)
  }
}

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

import java.util.Comparator

import com.intel.hibench.sparkbench.common.IOCommon
import com.intel.sparkbench.micro.TimSort
import org.apache.hadoop.examples.terasort.{TeraInputFormat, TeraOutputFormat}
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.BytesWritable
import org.apache.spark._
import org.apache.spark.rdd._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object ScalaTeraSort {
  implicit def rddToSampledOrderedRDDFunctions[K: Ordering : ClassTag, V: ClassTag]
  (rdd: RDD[(K, V)]) = new ConfigurableOrderedRDDFunctions[K, V, (K, V)](rdd)

  implicit def ArrayByteOrdering: Ordering[Array[Byte]] = Ordering.fromLessThan {
    case (a, b) => (new BytesWritable(a).compareTo(new BytesWritable(b))) < 0
  }

  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println(
        s"Usage: $ScalaTeraSort <INPUT_HDFS> <OUTPUT_HDFS>"
      )
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("ScalaTeraSort")
    val sc = new SparkContext(sparkConf)
    val io = new IOCommon(sc)

    //val file = io.load[String](args(0), Some("Text"))
    val data = sc.newAPIHadoopFile[Text, Text, TeraInputFormat](args(0)).map {
      case (k,v) => (k.copyBytes, v.copyBytes)
    }
    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)
    val reducer  = IOCommon.getProperty("hibench.default.shuffle.parallelism")
      .getOrElse((parallel / 2).toString).toInt

    val nvmfEnabled = sc.getConf.getBoolean("spark.shuffle.nvme.enabled", false)

    val partitioner = new BaseRangePartitioner(partitions = reducer, rdd = data)

//    val partitioned_sorted_rdd = data.map(kv => (partitioner.getPartition(kv._1), (kv._1, kv._2)))
//      .mapPartitions{ iterator =>
//        iterator.toSeq
//                .sortWith((r1, r2) => partitionKeyCompare((r1._1, r1._2._1), (r2._1, r2._2._1)) < 0)
//                .toIterator
//      }

    val partitioned_sorted_rdd = data.map(kv => (partitioner.getPartition(kv._1), (kv._1, kv._2)))
      .mapPartitions { iterator =>
        val map = new mutable.HashMap[Int, ArrayBuffer[(Array[Byte], Array[Byte])]]()
        iterator.foreach { case (pid, kv) =>
            val arr = map.getOrElseUpdate(pid, new ArrayBuffer[(Array[Byte], Array[Byte])]())
            arr += kv
        }

        map.foreach{ case (pid, kv) =>
            kv.sortWith((r1, r2) => keyCompare(r1._1, r2._1) < 0)
        }

        val arrBuffer = new ArrayBuffer[(Int, (Array[Byte], Array[Byte]))]()
        map.foreach { case (pid, kvArr) =>
            kvArr.foreach { case (k, v) =>
                arrBuffer += ((pid, (k ,v)))
            }
        }
        arrBuffer.toIterator
      }

    val ordered_rdd = new TeraSortPairRDDFunctions(partitioned_sorted_rdd)

    val hashPartitioner = new HashPartitioner(partitioner.numPartitions)

    val grouped_rdd = ordered_rdd.groupByKey(hashPartitioner)

    if (nvmfEnabled) {
      grouped_rdd.flatMap(_._2.toIterator)
        .map { case (k, v) => (new Text(k), new Text(v))}
        .saveAsNewAPIHadoopFile[TeraOutputFormat](args(1))
    } else {
      grouped_rdd.flatMap{ tuple =>
        val value = tuple._2.toArray

        TimSort.sort(value,
                     0,
                     value.length,
                     keyComparator(),
                     new Array[(Array[Byte], Array[Byte])](value.length),
                     value.length)

        value
      }.map{case (k, v) => (new Text(k), new Text(v))}
        .saveAsNewAPIHadoopFile[TeraOutputFormat](args(1))

    }

    sc.stop()
  }

  def keyComparator(): Comparator[(Array[Byte], Array[Byte])] = {
    new Comparator[(Array[Byte], Array[Byte])] {
      override def compare(a: (Array[Byte], Array[Byte]), b: (Array[Byte], Array[Byte])): Int = {
        keyCompare(a._1, b._1)
      }
    }
  }


  def keyCompare(a: Array[Byte], b: Array[Byte]): Int = {
    val bytesWritable1 = new BytesWritable(a)
    val bytesWritable2 = new BytesWritable(b)
    if (bytesWritable1.compareTo(bytesWritable2) < 0) -1 else 1
  }

  def partitionKeyCompare(a: (Int, Array[Byte]), b: (Int, Array[Byte])): Int = {
    val partitionDiff = a._1 - b._1
    if (partitionDiff != 0) {
      partitionDiff
    } else {
      keyCompare(a._2, b._2)
    }
  }
}

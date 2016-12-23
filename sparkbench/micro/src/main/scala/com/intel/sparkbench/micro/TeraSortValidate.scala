package com.intel.sparkbench.micro

import org.apache.hadoop.io.WritableComparator
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object TeraSortValidate {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.println(
        s"Usage: $TeraSortValidate <INPUT_HDFS>"
      )
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("TeraSortValidate")
    val sc = new SparkContext(conf)

    val inputData = sc.hadoopFile[Array[Byte], Array[Byte], TeraInputFormat](args(0))
      .map { case (k, v) =>
        (k.clone(), v.clone())
      }

    var first: Array[Byte] = null
    var last: (Array[Byte], Array[Byte]) = null
    var isFirst = true
    val collectData = inputData.mapPartitionsWithIndex{ (id, iterator) =>
      val hashMap = new mutable.HashMap[(Int, Array[Byte]), Array[Byte]]()
      iterator.foreach { kv =>
        if (isFirst) {
          first = kv._1
          last = kv
          isFirst = false
          hashMap += (id, kv._1) -> kv._2
          println(id + " first " + new String(first))
        } else {
          if (innerCompare(kv._1, last._1) < 0) {
            hashMap += (id, "error".getBytes) -> ("ERROR: " + kv._1).getBytes
          } else {
            last = kv
          }
        }
      }

      if (last != null) {
        hashMap += (id, last._1) -> last._2
        println(id + " last " + new String(last._1))
      }
      hashMap.toIterator
    }.collect().sortWith((a, b) => kvCompare(a, b) < 0)

    collectData.foreach(kv => println(kv._1._1 + " " + new String(kv._1._2)))

    collectData.filter(kv => innerCompare("error".getBytes, kv._1._2) == 0).foreach(println)



    for(i <- 0 until collectData.length - 1) {
      if (innerCompare(collectData(i)._1._2, collectData(i + 1)._1._2) > 0) {
        println(i + " " + new String(collectData(i)._1._2) + " " + new String(collectData(i + 1)._1._2))
      }
    }

    sc.stop()

  }

  def innerCompare(a: Array[Byte], b: Array[Byte]): Int = {
    if (a eq b) return 0
    WritableComparator.compareBytes(a, 0, a.length, b, 0, b.length)
  }

  type T = ((Int, Array[Byte]), Array[Byte])
  def kvCompare(a: T, b: T): Int = {
    if (a._1._1 < b._1._1) {
      -1
    } else if (a._1._1 > b._1._1) {
      1
    } else {
      innerCompare(a._1._2, b._1._2)
    }
  }
}

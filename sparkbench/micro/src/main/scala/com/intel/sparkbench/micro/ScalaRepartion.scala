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

package com.intel.sparkbench.micro

import com.intel.hibench.sparkbench.common.IOCommon
import org.apache.hadoop.examples.terasort.{TeraInputFormat, TeraOutputFormat}
import org.apache.hadoop.io.Text
import org.apache.spark.{SparkConf, SparkContext}


object ScalaRepartion {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println(
        s"Usage: $ScalaRepartion <INPUT_HDFS> <OUTPUT_HDFS>"
      )
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("ScalaTeraSort")
    val sc = new SparkContext(sparkConf)

    //val file = io.load[String](args(0), Some("Text"))
    val data = sc.newAPIHadoopFile[Text, Text, TeraInputFormat](args(0))

    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)
    val reducer  = IOCommon.getProperty("hibench.default.shuffle.parallelism")
      .getOrElse((parallel / 2).toString).toInt

    data.repartition(reducer).saveAsNewAPIHadoopFile[TeraOutputFormat](args(1))

    sc.stop()
  }

}


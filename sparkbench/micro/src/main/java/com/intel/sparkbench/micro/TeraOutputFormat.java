package com.intel.sparkbench.micro;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * A streamlined text output format that writes key, value, and "\r\n".
 */
public class TeraOutputFormat extends TextOutputFormat<byte[],byte[]> {
  static final String FINAL_SYNC_ATTRIBUTE = "terasort.final.sync";

  /**
   * Set the requirement for a final sync before the stream is closed.
   */
  public static void setFinalSync(JobConf conf, boolean newValue) {
    conf.setBoolean(FINAL_SYNC_ATTRIBUTE, newValue);
  }

  /**
   * Does the user want a final sync at close?
   */
  public static boolean getFinalSync(JobConf conf) {
    return conf.getBoolean(FINAL_SYNC_ATTRIBUTE, false);
  }

  static class TeraRecordWriter extends LineRecordWriter<byte[],byte[]> {
    private static final byte[] newLine = "\r\n".getBytes();
    private boolean finalSync = false;

    public TeraRecordWriter(DataOutputStream out,
                            JobConf conf) {
      super(out);
      finalSync = getFinalSync(conf);
    }

    public TeraRecordWriter(DataOutputStream out,
                            String keyValueSeparator,
                            JobConf conf) {
      super(out, keyValueSeparator);
      finalSync = getFinalSync(conf);
    }

    public synchronized void write(byte[] key,
                                   byte[] value) throws IOException {
      out.write(key, 0, key.length);
      out.write(value, 0, value.length);
      out.write(newLine, 0, newLine.length);
    }

    public void close() throws IOException {
      if (finalSync) {
        ((FSDataOutputStream) out).sync();
      }
      super.close(null);
    }
  }

  public RecordWriter<byte[],byte[]> getRecordWriter(FileSystem ignored,
                                                 JobConf job,
                                                 String name,
                                                 Progressable progress
  ) throws IOException {
//    Path dir = getWorkOutputPath(job);
//    FileSystem fs = dir.getFileSystem(job);
//    FSDataOutputStream fileOut = fs.create(new Path(dir, name), progress);
//    return new TeraRecordWriter(fileOut, job);


    boolean isCompressed = getCompressOutput(job);
    String keyValueSeparator = job.get("mapreduce.output.textoutputformat.separator",
      "\t");
    if (!isCompressed) {
      Path file = FileOutputFormat.getTaskOutputPath(job, name);
      FileSystem fs = file.getFileSystem(job);
      FSDataOutputStream fileOut = fs.create(file, progress);
      return new TeraRecordWriter(fileOut, keyValueSeparator, job);
    } else {
      Class<? extends CompressionCodec> codecClass =
        getOutputCompressorClass(job, GzipCodec.class);
      // create the named codec
      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, job);
      // build the filename including the extension
      Path file =
        FileOutputFormat.getTaskOutputPath(job,
          name + codec.getDefaultExtension());
      FileSystem fs = file.getFileSystem(job);
      FSDataOutputStream fileOut = fs.create(file, progress);
      return new TeraRecordWriter(new DataOutputStream
        (codec.createOutputStream(fileOut)),
        keyValueSeparator, job);
    }
  }
}
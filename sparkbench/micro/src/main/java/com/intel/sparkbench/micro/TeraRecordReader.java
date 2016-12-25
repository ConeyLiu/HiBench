package com.intel.sparkbench.micro;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

/**
 * Created by lxy on 2016/12/22.
 */
public class TeraRecordReader implements RecordReader<byte[],byte[]> {
  private LineRecordReader in;
  private LongWritable junk = new LongWritable();
  private Text line = new Text();
  private static int KEY_LENGTH = 10;
  private static int VALUE_LENGTH = 90;
  public TeraRecordReader(Configuration job,
                          FileSplit split) throws IOException {
    in = new LineRecordReader(job, split);
  }
  public void close() throws IOException {
    in.close();
  }
  public byte[] createKey() {
    return new byte[KEY_LENGTH];
  }
  public byte[] createValue() {
    return new byte[VALUE_LENGTH];
  }
  public long getPos() throws IOException {
    return in.getPos();
  }
  public float getProgress() throws IOException {
    return in.getProgress();
  }
  public boolean next(byte[] key, byte[] value) throws IOException {
    if (in.next(junk, line)) {
      if (line.getLength() < KEY_LENGTH) {
//        key.set(line);
//        value.clear();
        System.arraycopy(line.getBytes(), 0, key, 0, line.getLength());
      } else {
        byte[] bytes = line.getBytes();
//        key.set(bytes, 0, KEY_LENGTH);
//        value.set(bytes, KEY_LENGTH, line.getLength() - KEY_LENGTH);
        System.arraycopy(bytes, 0, key, 0, KEY_LENGTH);
        //System.out.println("----->" + bytes.length + " : " + (line.getLength() - KEY_LENGTH));
        System.arraycopy(bytes, KEY_LENGTH, value, 0, line.getLength() - KEY_LENGTH);
      }
      return true;
    } else {
      return false;
    }
  }
}

package com.intel.sparkbench.micro;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;


@SuppressWarnings("deprecation")
public class LzoLineRecordReader implements RecordReader<byte[], byte[]> {
  private CompressionCodecFactory codecFactory = null;
  private long start;
  private long pos;
  private final long end;
  private final LineReader in;
  private final FSDataInputStream fileIn;
  private static int KEY_LENGTH = 10;
  private static int VALUE_LENGTH = 90;
  private Text line = new Text();

  LzoLineRecordReader(Configuration conf, FileSplit split) throws IOException {
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();

    FileSystem fs = file.getFileSystem(conf);
    codecFactory = new CompressionCodecFactory(conf);
    final CompressionCodec codec = codecFactory.getCodec(file);
    if (codec == null) {
      throw new IOException("No LZO codec found, cannot run.");
    }

    // Open the file and seek to the next split.
    fileIn = fs.open(file);
    // Create input stream and read the file header.
    in = new LineReader(codec.createInputStream(fileIn), conf);
    if (start != 0) {
      fileIn.seek(start);

      // Read and ignore the first line.
      in.readLine(new Text());
      start = fileIn.getPos();
    }

    pos = start;
  }

  public byte[] createKey() {
    return new byte[KEY_LENGTH];
  }

  public byte[] createValue() {
    return new byte[VALUE_LENGTH];
  }

  public boolean next(byte[] key, byte[] value) throws IOException {
    // Since the LZOP codec reads everything in LZO blocks, we can't stop if pos == end.
    // Instead, wait for the next block to be read in when pos will be > end.
    while (pos <= end) {
      //key.set(pos);

      int newSize = in.readLine(line);

      if (newSize == 0) {
        return false;
      }

      if (line.getLength() < KEY_LENGTH) {
//        key.set(line);
//        value.clear();
        System.arraycopy(line.getBytes(), 0, key, 0, line.getLength());
      } else {
        byte[] bytes = line.getBytes();
//        key.set(bytes, 0, KEY_LENGTH);
//        value.set(bytes, KEY_LENGTH, line.getLength() - KEY_LENGTH);
        System.arraycopy(bytes, 0, key, 0, KEY_LENGTH);
        System.arraycopy(bytes, KEY_LENGTH, value, 0, line.getLength() - KEY_LENGTH);
      }

      pos = fileIn.getPos();
      return true;
    }
    return false;
  }

  public float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start)/ (float)(end - start));
    }
  }

  public synchronized long getPos() throws IOException {
    return pos;
  }

  public synchronized void close() throws IOException {
    if (in != null) {
      in.close();
    }
  }
}
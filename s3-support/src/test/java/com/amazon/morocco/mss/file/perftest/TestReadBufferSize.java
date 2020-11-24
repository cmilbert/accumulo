package com.amazon.morocco.mss.file.perftest;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

// this is a performance test to evaluate latency and throughput when reading from an S3 object using S3InputStream
// execute from the root of this project with:
//     $(brazil-path tooldirect.jdk)/bin/java -cp $(brazil-path testrun.classpath):build/private/classes/tests/
//         com.amazon.morocco.mss.file.perftest.TestReadBufferSize 'accS3nf://bucket/path/file'
public class TestReadBufferSize {

  private static long testFileLength = 0;

  private static FSDataInputStream getTestFileInputStream(int bufferSize, Path testFilePath)
      throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(testFilePath.toUri(), conf);
    testFileLength = fs.getFileStatus(testFilePath).getLen();
    return fs.open(testFilePath, bufferSize);
  }

  public static void main(String[] args) throws Exception {
    String testFile = args[0];
    Path testFilePath = new Path(testFile);

    System.out.println("Starting variable sequential reads");
    for (int trial = 0; trial < 3; trial++) {
      for (int bufferSize : new int[] {1 << 12, 1 << 13, 1 << 14, 1 << 15, 1 << 16, 1 << 17,
          1 << 18, 1 << 19, 1 << 20, 1 << 21, 1 << 22, 1 << 23, 1 << 24}) {
        double bytesPerSecond = test(bufferSize, testFilePath);
        System.out.println(bufferSize + ", " + bytesPerSecond);
      }
    }
    System.out.println("Starting long sequential read");
    for (int trial = 0; trial < 3; trial++) {
      double throughput = timeSequentialAccess(testFilePath);
      System.out.println(throughput);
    }
    System.out.println("Starting random reads");
    for (int trial = 0; trial < 3; trial++) {
      double seekTime = timeRandomAccess(testFilePath);
      System.out.println(seekTime);
    }

  }

  public static double test(int bufferSize, Path testFilePath) throws Exception {

    long startTime = System.currentTimeMillis();
    long totalRead = 0;
    try (FSDataInputStream in = getTestFileInputStream(bufferSize, testFilePath)) {
      byte[] buffer = new byte[1 << 10];
      int read = 0;
      while ((read = in.read(buffer)) > 0) {
        // if((totalRead + read)/(1<<20) != totalRead/(1<<20)) {
        // System.out.println("progress: "+(totalRead + read));
        // }
        totalRead += read;
        if (totalRead > bufferSize << 8 || totalRead > 1 << 26) {
          break;
        }
      }
    }
    long endTime = System.currentTimeMillis();

    // System.out.println("Buffer: "+bufferSize+" Read "+totalRead+" bytes in "+(endTime -
    // startTime)+"ms");
    return (double) totalRead / (double) (endTime - startTime) * 1000;
  }

  public static double timeSequentialAccess(Path testFilePath) throws IOException {
    long startTime = System.currentTimeMillis();
    long totalRead = 0;
    try (FSDataInputStream in = getTestFileInputStream(0, testFilePath)) {
      byte[] buffer = new byte[1 << 13];
      while (totalRead < 1 << 28) {
        int read = in.read(buffer);
        if (read <= 0) {
          throw new IOException("end of file reached prematurely");
        }
        totalRead += read;
      }
    }
    long endTime = System.currentTimeMillis();
    return totalRead / (double) (endTime - startTime);
  }

  public static double timeRandomAccess(Path testFilePath) throws IOException {
    long startTime = System.currentTimeMillis();
    Random r = new Random();
    try (FSDataInputStream in = getTestFileInputStream(0, testFilePath)) {
      byte[] buffer = new byte[1 << 13];
      for (int seek = 0; seek < 1000; seek++) {
        long position = (r.nextLong() & Long.MAX_VALUE) % testFileLength;
        in.seek(position);
        int read = in.read(buffer);
      }
    }
    long endTime = System.currentTimeMillis();
    return (endTime - startTime) / 1000.0;
  }

}

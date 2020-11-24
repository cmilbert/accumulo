package com.amazon.morocco.mss.file;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.mockito.AdditionalAnswers;

import com.amazon.morocco.util.java.ThrowingRunnable;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.google.common.util.concurrent.Uninterruptibles;

public class AccumuloNoFlushS3FileSystemTest extends MockS3TestBase {

  public AccumuloNoFlushS3FileSystemTest() {
    super("accS3nf");
  }

  @Test
  public void testSmallReadWriteOperations() throws Exception {
    final AmazonS3 amazonS3 = mock(AmazonS3.class);
    // testReadWriteOperations(amazonS3, 3, () -> {
    // verify(amazonS3, times(1)).putObject(any(PutObjectRequest.class));
    // });
  }

  @Test
  public void testLargeReadWriteOperations() throws Exception {
    final AmazonS3 amazonS3 = mock(AmazonS3.class);
    // testReadWriteOperations(amazonS3, 6, () -> {
    // verify(amazonS3,
    // times(1)).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
    // verify(amazonS3, times(2)).uploadPart(any(UploadPartRequest.class));
    // verify(amazonS3,
    // times(1)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
    // });
  }

  public void testReadWriteOperations(AmazonS3 amazonS3, int writeMBs, ThrowingRunnable validation)
      throws Exception {
    when(amazonS3.getObjectMetadata(anyString(), anyString()))
        .then(AdditionalAnswers.delegatesTo(s3));
    when(amazonS3.getObject(any(GetObjectRequest.class))).then(AdditionalAnswers.delegatesTo(s3));
    when(amazonS3.putObject(any(PutObjectRequest.class))).then(AdditionalAnswers.delegatesTo(s3));
    when(amazonS3.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
        .then(AdditionalAnswers.delegatesTo(s3));
    when(amazonS3.uploadPart(any(UploadPartRequest.class))).then(AdditionalAnswers.delegatesTo(s3));
    when(amazonS3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
        .then(AdditionalAnswers.delegatesTo(s3));
    s3 = amazonS3;
    FileSystem fs = getFileSystem();
    Path testFile = new Path("/test/file");

    // minimum buffer is 5MB so we need to write 6 times
    byte[] testData = new byte[1 << 20];
    Random r = new Random();
    r.nextBytes(testData);

    FSDataOutputStream out = fs.create(testFile);
    for (int i = 0; i < writeMBs; i++) {
      out.write(testData, 0, testData.length);
      // Add a small delay after each loop to let any triggered flush task get far enough along
      Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
    }
    out.close();

    validation.run();

    assertEquals(testData.length * writeMBs, fs.getFileStatus(testFile).getLen());
    try (FSDataInputStream in = fs.open(testFile)) {
      byte[] readData = new byte[testData.length];
      for (int j = 0; j < writeMBs - 1; j++) {
        int bytesRead = 0;
        while (bytesRead < testData.length) {
          int i = in.read(readData, bytesRead, readData.length - bytesRead);
          assertTrue("expected to read at least one byte", i > 0);
          bytesRead += i;
        }
        assertArrayEquals(testData, readData);
      }
      in.readFully(readData);
      assertArrayEquals(testData, readData);

      // do some random offset reads
      for (int i = 0; i < 10; i++) {
        int readLength = 17;
        int offset = r.nextInt(testData.length - 2 * readLength);
        byte[] randomReadBuffer = new byte[readLength];
        in.seek(offset);
        in.readFully(randomReadBuffer);
        byte[] expected = new byte[readLength];
        System.arraycopy(testData, offset, expected, 0, readLength);
        assertArrayEquals(expected, randomReadBuffer);
        in.readFully(randomReadBuffer);
        System.arraycopy(testData, offset + readLength, expected, 0, readLength);
        assertArrayEquals(expected, randomReadBuffer);
      }
    }
  }

  // TODO: test failure handling
  // TODO: test S3 inconsistency (e.g one AZ unavailable temporarily)

}

package org.apache.accumulo.s3.file;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.mockito.AdditionalAnswers;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class AccumuloMultiObjectS3FileSystemTest extends MockS3TestBase {

  public AccumuloMultiObjectS3FileSystemTest() {
    super("accS3mo");
  }

  @Test
  public void testFSOperations() throws IOException {

    FileSystem fs = getFileSystem();
    Path testDirPath = new Path("/test");
    Path testFilePath = new Path("/test/file");
    Path testFile2Path = new Path("/test/file2");
    Path testFileEmpty = new Path("/test/empty");
    Path testDirEmpty = new Path("/test/emptyDir");

    FSDataOutputStream out = fs.create(testFilePath);
    // write small
    byte[] buffer = new byte[1 << 23];
    Random r = new Random();
    r.nextBytes(buffer);
    out.write(buffer[0] & 0xFF);
    // flush
    out.flush();
    // write big
    out.write(buffer, 1, buffer.length - 1);
    // flush
    out.flush();
    // write big
    byte[] buffer2 = new byte[1 << 23];
    r.nextBytes(buffer2);
    out.write(buffer2, 0, buffer2.length - 1);
    out.write(buffer2[buffer2.length - 1]);
    // close
    out.close();

    byte[] readBuffer;
    try (FSDataInputStream in = fs.open(testFilePath)) {
      // read to buffer
      readBuffer = new byte[buffer.length * 2];
      in.readFully(readBuffer);
      // validate contents
      assertArrayEquals("buffer read matches first buffer write", buffer,
          Arrays.copyOfRange(readBuffer, 0, buffer.length));
      assertArrayEquals("buffer read matches second buffer write", buffer2,
          Arrays.copyOfRange(readBuffer, buffer.length, readBuffer.length));
    }

    // exist
    assertTrue("testing file existence", fs.exists(testFilePath));
    assertFalse("testing file non-existence", fs.exists(testFile2Path));
    // uri operations
    // stat file
    FileStatus stat = fs.getFileStatus(testFilePath);
    assertTrue("file stat should indicate file", stat.isFile());
    // stat "directory"
    FileStatus[] stats = fs.listStatus(testDirPath);
    assertEquals("should have one file in the directory", 1, stats.length);
    // operations on empty directory
    // operations on empty file
    out = fs.create(testFileEmpty);
    out.close();
    FileStatus statE = fs.getFileStatus(testFileEmpty);
    assertEquals("empty file length", 0, statE.getLen());
    assertTrue(statE.isFile());
    // read an empty file
    try (FSDataInputStream in = fs.open(testFileEmpty)) {
      assertEquals("no characters can be read from empty file", -1, in.read());
    }
    try {
      fs.open(testFile2Path);
      fail("should get exception opening non-existent file");
    } catch (FileNotFoundException e) {} catch (IOException e) {
      fail("caught exception other than FileNotFoundException for non-existent file: " + e);
    }

    fs.createNewFile(new Path("test2/mapfile/data"));
    fs.createNewFile(new Path("test2/mapfile/index"));

    stats = fs.listStatus(new Path("test2/"));

    assertEquals(1, stats.length);
    assertEquals("mapfile", stats[0].getPath().getName());

    fs.mkdirs(testDirEmpty);

    assertTrue(fs.exists(testDirEmpty));
    assertTrue(fs.getFileStatus(testDirEmpty).isDirectory());
    FileStatus[] status = fs.listStatus(testDirEmpty);
    assertTrue("expecting no children in empty directory", status.length == 0);

    FileStatus[] globStatuses = fs.globStatus(new Path("/test/*"));
    assertEquals(3, globStatuses.length);

    // delete (file)
    fs.delete(testFileEmpty, false);
    assertFalse(fs.exists(testFileEmpty));
    // delete (dir)
    fs.delete(testDirEmpty, true);
    assertFalse(fs.exists(testDirEmpty));
    // recursive delete
    fs.delete(testDirPath, true);
    assertFalse(fs.exists(testDirPath));

    Path qualified = fs.makeQualified(testFilePath);
    assertEquals(scheme, qualified.toUri().getScheme());
    assertEquals(bucketName, qualified.toUri().getAuthority());

  }

  @Test
  public void testFlushingWithErrors() throws Exception {
    AtomicInteger counter = new AtomicInteger(0);
    s3 = failTwiceWrapper(s3, counter);
    FileSystem fs = getFileSystem();
    Path testFilePath = new Path("/failTest");
    FSDataOutputStream out = fs.create(testFilePath);
    byte[] buffer1 = new byte[1234];
    Random r = new Random();
    r.nextBytes(buffer1);
    out.write(buffer1);

    counter.set(2);

    out.flush();
    byte[] buffer2 = new byte[2345];
    r.nextBytes(buffer2);
    out.write(buffer2);

    counter.set(2);

    out.close();
    FileStatus stat = fs.getFileStatus(testFilePath);
    assertEquals(buffer1.length + buffer2.length, stat.getLen());
    byte[] input = Arrays.copyOf(buffer1, buffer1.length + buffer2.length);
    System.arraycopy(buffer2, 0, input, buffer1.length, buffer2.length);
    byte[] output = new byte[(int) stat.getLen()];
    try (FSDataInputStream in = fs.open(testFilePath)) {
      in.readFully(output);
    }
    assertArrayEquals(input, output);
  }

  private AmazonS3 failTwiceWrapper(final AmazonS3 s3, final AtomicInteger counter) {
    final AmazonS3 amazonS3 = mock(AmazonS3.class);
    when(amazonS3.putObject(any(PutObjectRequest.class))).then(answer -> {
      if (counter.getAndDecrement() <= 0) {
        return s3.putObject(answer.getArgument(0));
      } else {
        throw new IOException("injected exception");
      }
    });
    when(amazonS3.listObjects(anyString(), anyString())).then(AdditionalAnswers.delegatesTo(s3));
    when(amazonS3.listObjects(any(ListObjectsRequest.class)))
        .then(AdditionalAnswers.delegatesTo(s3));
    when(amazonS3.getObject(any(GetObjectRequest.class))).then(AdditionalAnswers.delegatesTo(s3));
    return amazonS3;
  }

  @Test
  public void testCreateWithNoClose() throws Exception {
    FileSystem fs = getFileSystem();
    Path testFile = new Path("createWithNoClose");
    try (FSDataOutputStream created = fs.create(testFile)) {
      // File was created but not closed, which means we should be able to open the file and find no
      // contents
      FSDataInputStream open = fs.open(testFile);
      byte[] buffer = new byte[8]; // just some small buffer size
      assertEquals(-1, open.read(buffer)); // end of the stream has been reached
      assertTrue(fs.exists(testFile));
    }
  }

  // @Test
  // public void testCreateWithBadBucket() {
  // FileSystem fs = getFileSystem();
  // Path testFile = new Path("badBucketFile");
  // s3.deleteBucket(bucketName);
  //
  // expectException(IOException.class, () -> fs.create(testFile));
  // }
  //
  // @Test
  // public void testOpenNonExistantFile() {
  // FileSystem fs = getFileSystem();
  // Path testFile = new Path("uncreatedFile");
  // expectException(FileNotFoundException.class, () -> fs.open(testFile));
  // }
}

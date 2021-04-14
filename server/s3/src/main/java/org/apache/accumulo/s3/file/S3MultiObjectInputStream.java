package org.apache.accumulo.s3.file;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class S3MultiObjectInputStream extends InputStream implements Seekable, PositionedReadable {

  private final S3ClientWrapper s3;
  private final String bucketName;
  private final String objectPrefix;
  private long pos = 0;
  private long bufferStart = 0;
  private int objectId = 0;
  private byte[] buffer;
  private boolean endReached = false;

  public S3MultiObjectInputStream(S3ClientWrapper s3, String bucketName, String objectName)
      throws IOException {
    this.s3 = s3;
    this.bucketName = bucketName;
    this.objectPrefix = objectName + "/" + AccumuloMultiObjectS3FileSystem.partPrefix;
    fillBuffer();
    if (buffer == null) {
      throw new FileNotFoundException();
    }
  }

  private synchronized void fillBuffer() throws IOException {
    if (endReached) {
      return;
    }
    if (buffer != null) {
      bufferStart += buffer.length;
    }
    String nextObjectName = objectPrefix + objectId++;
    GetObjectRequest req = new GetObjectRequest(bucketName, nextObjectName);
    S3Object object = s3.getObject(req);
    if (object == null) {
      buffer = null;
      endReached = true;
      return;
    }
    long length = object.getObjectMetadata().getContentLength();
    buffer = new byte[(int) length];
    try (S3ObjectInputStream content = object.getObjectContent()) {
      int offset = 0;
      while (offset < buffer.length) {
        int read = content.read(buffer, offset, buffer.length - offset);
        if (read <= 0) {
          throw new IOException("read terminated before end of object");
        }
        offset += read;
      }
    } finally {
      object.close();
    }
  }

  @Override
  public synchronized int read() throws IOException {
    if (endReached) {
      return -1;
    }
    while (buffer == null || pos >= bufferStart + buffer.length) {
      fillBuffer();
      if (endReached) {
        return -1;
      }
    }
    return buffer[(int) (pos++ - bufferStart)] & 0xFF;
  }

  @Override
  public synchronized int read(final byte[] b, final int offset, final int length)
      throws IOException {
    int off = offset;
    int len = length;
    int totalRead = 0;
    while (len > 0) {
      int bufferPos = (int) (pos - bufferStart);
      if (buffer == null || bufferPos >= buffer.length) {
        fillBuffer();
        if (endReached) {
          return totalRead > 0 ? totalRead : -1;
        }
        continue;
      }
      int avilableToRead = buffer.length - bufferPos;
      int read = Math.min(len, avilableToRead);
      System.arraycopy(buffer, bufferPos, b, off, read);
      len -= read;
      off += read;
      pos += read;
      totalRead += read;
    }
    return totalRead;
  }

  public void seek(long pos) throws IOException {
    // throw new UnsupportedOperationException();
  }

  public long getPos() throws IOException {
    return pos;
  }

  public boolean seekToNewSource(long targetPos) throws IOException {
    throw new UnsupportedOperationException();
  }

  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void readFully(long position, byte[] buffer) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {}

}

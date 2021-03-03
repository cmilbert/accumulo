package org.apache.accumulo.s3.file;

public class S3NoFlushMultipartOutputStream extends S3OutputStreamBase {

  public S3NoFlushMultipartOutputStream(S3ClientWrapper s3, String bucketName, String objectName,
      int bufferSize) {
    super(s3, bucketName, objectName, bufferSize);
  }

  @Override
  protected byte[] copyToFlushBuffer(byte[] circularBuffer, long startPos, long endPos) {
    byte[] flushBuffer = new byte[(int) (endPos - startPos)];
    int circularStart = (int) (startPos % circularBuffer.length);
    int circularEnd = (int) (endPos % circularBuffer.length);
    if (circularStart < circularEnd || circularEnd == 0) {
      System.arraycopy(circularBuffer, circularStart, flushBuffer, 0, flushBuffer.length);
    } else {
      int firstCopyLength = circularBuffer.length - circularStart;
      System.arraycopy(circularBuffer, circularStart, flushBuffer, 0, firstCopyLength);
      System.arraycopy(circularBuffer, 0, flushBuffer, firstCopyLength,
          flushBuffer.length - firstCopyLength);
    }
    return flushBuffer;
  }

  @Override
  public void flush() {
    // do nothing
  }
}

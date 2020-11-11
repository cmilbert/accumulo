package com.amazon.morocco.mss.file;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class S3InputStream extends InputStream implements Seekable, PositionedReadable {

    private static final Logger log = LoggerFactory.getLogger(S3InputStream.class);

    private final S3ClientWrapper s3;
    private final String bucketName;
    private final String objectName;
    private final long objectLength;
    private byte[] buffer = null;
    private long bufferStart = 0;
    private long bufferEnd = 0;
    private long position = 0;
    private final int minBufferSize;
    private final static int MIN_BUFFER_SIZE_DEFAULT = 1 << 18;
    public final static String MIN_BUFFER_SIZE_KEY = "com.amazon.morocco.s3.read.buffer.min";
    private final int maxBufferSize;
    private final static int MAX_BUFFER_SIZE_DEFAULT = 1 << 24;
    public final static String MAX_BUFFER_SIZE_KEY = "com.amazon.morocco.s3.read.buffer.max";
    private final Configuration conf;

    public static class ExtraBufferAllocationTracker {

        private final static long MAX_EXTRA_BUFFER_TO_ALLOCATE_DEFAULT = 1L << 30;
        public final static String MAX_EXTRA_BUFFER_TO_ALLOCATE_KEY = "com.amazon.morocco.s3.read.buffer.allocation.limit";

        private static long totalExtraBufferAllocated = 0;
        private static long maxExtraBufferToAllocate = -1;

        private synchronized static boolean allocate(int size, Configuration conf) {
            if (maxExtraBufferToAllocate < 0) {
                maxExtraBufferToAllocate = conf.getLong(MAX_EXTRA_BUFFER_TO_ALLOCATE_KEY, MAX_EXTRA_BUFFER_TO_ALLOCATE_DEFAULT);
            }
            if (totalExtraBufferAllocated + size > maxExtraBufferToAllocate) {
                return false;
            }
            totalExtraBufferAllocated += size;
            return true;
        }

        private synchronized static void deAllocate(int size) {
            totalExtraBufferAllocated -= size;
        }

        public synchronized static long getCurrentExtraBufferAllocation() {
            return totalExtraBufferAllocated;
        }

        @VisibleForTesting
        synchronized static void resetMaxExtraBufferToAllocate() {
            maxExtraBufferToAllocate = -1;
        }
    }

    public S3InputStream(S3ClientWrapper s3, String bucketName, String objectName, Configuration conf) throws IOException {
        this.conf = conf;
        this.minBufferSize = conf.getInt(MIN_BUFFER_SIZE_KEY, MIN_BUFFER_SIZE_DEFAULT);
        this.maxBufferSize = conf.getInt(MAX_BUFFER_SIZE_KEY, MAX_BUFFER_SIZE_DEFAULT);
        this.s3 = s3;
        this.bucketName = bucketName;
        this.objectName = objectName;
        ObjectMetadata meta = s3.getObjectMetadata(bucketName, objectName);
        if (meta == null) {
            throw new IOException(String.format("object %s:%s not found", bucketName, objectName));
        }
        objectLength = meta.getInstanceLength();
    }

    private void fillBufferFully(byte[] buffer, int inputOff, int inputLen, long objectPosition) throws IOException {
        int off = inputOff;
        int len = inputLen;
        long rangeEnd = objectPosition + len - 1;
        try (S3Object object = s3.getObject(new GetObjectRequest(bucketName, objectName).withRange(objectPosition, rangeEnd))) {
            if (object == null) {
                throw new IOException(String.format("object %s:%s (range %d:%d) not found", bucketName, objectName, objectPosition, rangeEnd));
            }
            try (S3ObjectInputStream in = object.getObjectContent()) {
                while (len > 0) {
                    int read = in.read(buffer, off, len);
                    if (read < 0) {
                        throw new IOException("premature end of input");
                    }
                    off += read;
                    len -= read;
                }
            }
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void fillBuffer(long start) throws IOException {
        if (start >= objectLength) {
            throw new IOException("attempting to read past end of object: " + start + " >= " + objectLength);
        }
        int targetBufferSize = buffer != null ? buffer.length : minBufferSize;
        if (start >= bufferEnd && start - bufferEnd < targetBufferSize) {
            // we're reading mostly sequentially, so grow the buffer
            targetBufferSize = Math.min(maxBufferSize, targetBufferSize * 2);
        } else {
            // we're reading randomly, so shrink the buffer
            targetBufferSize = minBufferSize;
        }
        // back up the start to fill a buffer in anticipation of future reads, but don't reread data in the current buffer
        long offsetStart = Math.max(0L, Math.min(start, Math.max(bufferEnd, objectLength - minBufferSize)));
        int length = (int) Math.min(objectLength - offsetStart, targetBufferSize);
        // get minBufferSize allocated for free -- it will be tracked as part of MOROCCO-119
        int currentBufferAllocation = Math.max(minBufferSize, buffer == null ? 0 : buffer.length);
        if (length > currentBufferAllocation) {
            // try to allocate a larger buffer, but fall back on the current buffer size if we can't
            if (!ExtraBufferAllocationTracker.allocate(length - currentBufferAllocation, conf)) {
                length = currentBufferAllocation;
            }
        } else if (length < currentBufferAllocation && currentBufferAllocation > minBufferSize) {
            ExtraBufferAllocationTracker.deAllocate(currentBufferAllocation - Math.max(minBufferSize, length));
        }
        buffer = new byte[length];
        fillBufferFully(buffer, 0, length, offsetStart);
        bufferStart = offsetStart;
        bufferEnd = offsetStart + length;
    }

    @Override
    public int read() throws IOException {
        if (position >= objectLength) {
            return -1;
        }
        if (buffer == null || position < bufferStart || position >= bufferEnd) {
            fillBuffer(position);
        }
        int val = buffer[(int) (position - bufferStart)] & 0xFF;
        position++;
        return val;
    }

    @Override
    public int read(byte[] b, int inputOff, int inputLen) throws IOException {
        int off = inputOff;
        int len = inputLen;
        if (position >= objectLength) {
            return -1;
        }
        int totalRead = 0;
        // first, grab what we can from the buffer
        if (buffer != null && position >= bufferStart && position < bufferEnd) {
            int bufferCopyLength = Math.min(len, (int) (bufferEnd - position));
            System.arraycopy(buffer, (int) (position - bufferStart), b, off, bufferCopyLength);
            position += bufferCopyLength;
            off += bufferCopyLength;
            len -= bufferCopyLength;
            totalRead += bufferCopyLength;
        }
        // if there's at least a single buffer left to read then read it in one chunk
        if (len >= maxBufferSize) {
            int toRead = (int) Math.min((long) len, objectLength - position);
            fillBufferFully(b, off, toRead, position);
            position += toRead;
            totalRead += toRead;
        } else if (len > 0) {
            fillBuffer(position);
            // we might not have read a full bufferSize, like if we're at the end of the file
            int copyLength = Math.min(len, (int) (bufferEnd - position));
            System.arraycopy(buffer, (int) (position - bufferStart), b, off, copyLength);
            position += copyLength;
            totalRead += copyLength;
        }
        return totalRead;
    }

    @Override
    public void seek(long pos) throws IOException {
        position = pos;
    }

    @Override
    public long getPos() throws IOException {
        return position;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        this.position = position;
        if (position >= objectLength) {
            return -1;
        }
        return read(buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        // the DataInputStream doesn't proxy calls to readFully() -- they end up going to read()
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        readFully(position, buffer, 0, buffer.length);
    }

    private void deallocateBuffer() {
        int currentAllocation = buffer != null ? buffer.length - minBufferSize : 0;
        buffer = null;
        if (currentAllocation > 0) {
            ExtraBufferAllocationTracker.deAllocate(currentAllocation);
        }
    }

    @Override
    public void close() throws IOException {
        deallocateBuffer();
        super.close();
    }

    @Override
    protected void finalize() throws Throwable {
        if (buffer != null) {
            log.warn("Input buffer being cleaned up by finalize(). This is a performance problem.");
            deallocateBuffer();
        }
        super.finalize();
    }
}

package com.amazon.morocco.mss.file;

import com.amazonaws.services.s3.S3ResponseMetadata;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.util.Md5Utils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Syncable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;

@Slf4j
public abstract class S3OutputStreamBase extends OutputStream implements Syncable {

    private static final int S3_MINIMUM_PART_SIZE_MB = 5;
    private static final int S3_MINIMUM_PART_SIZE = S3_MINIMUM_PART_SIZE_MB << 20;

    private static final int DEFAULT_MINIMUM_PART_SIZE_MB = 5;
    private static final int DEFAULT_MINIMUM_PART_SIZE = DEFAULT_MINIMUM_PART_SIZE_MB << 20;
    private static final int MINIMUM_BUFFER_SIZE_MB = 15;
    protected static final int MINIMUM_BUFFER_SIZE = MINIMUM_BUFFER_SIZE_MB << 20;
    private static long mark = System.currentTimeMillis();

    protected final int minimumPartSize;
    private final S3ClientWrapper s3;
    private final String bucketName;
    private final String objectName;
    private final ExecutorService flushingService;
    private final byte[] circularWriteBuffer;
    private long pos = 0;
    private long previousFlushTarget = 0;
    private long maxCompletedFlushLevel = 0;
    private Set<FlushTask> runningFlushTasks = new HashSet<>();
    private boolean closed = false;
    private boolean uninitializedFlushSubmitted = false;
    private String uploadId = null;
    private int nextPartNumber = 1;
    private List<PartETag> partETags = new ArrayList<>();
    private final Object flushMonitor = new Object();
    private final Object writeOrderMonitor = new Object();
    private boolean isMultiPart = false;

    @VisibleForTesting
    protected Exception flusherException;
    // metrics
    private long totalFlushed = 0;
    private long totalFlushTasks = 0;
    private long totalFlushes = 0;

    public S3OutputStreamBase(S3ClientWrapper s3, String bucketName, String objectName) {
        this(s3, bucketName, objectName, 1, MINIMUM_BUFFER_SIZE, DEFAULT_MINIMUM_PART_SIZE);
    }

    public S3OutputStreamBase(S3ClientWrapper s3, String bucketName, String objectName, int bufferSize) {
        this(s3, bucketName, objectName, 1, bufferSize, bufferSize / 3);
    }

    public S3OutputStreamBase(S3ClientWrapper s3, String bucketName, String objectName, int numFlushThreads, int bufferSize, int minimumPartSize) {
        if (bufferSize < MINIMUM_BUFFER_SIZE) {
            log.warn("Buffer size configured below {} (was {}), defaulting to {}MB", MINIMUM_BUFFER_SIZE, bufferSize, MINIMUM_BUFFER_SIZE_MB);
        }
        int actualBufferSize = Math.max(bufferSize, MINIMUM_BUFFER_SIZE);

        this.minimumPartSize = Math.max(minimumPartSize, S3_MINIMUM_PART_SIZE);
        if (minimumPartSize < S3_MINIMUM_PART_SIZE) {
            log.warn("Minimum part size configured below {}, defaulting to {}MB", S3_MINIMUM_PART_SIZE, S3_MINIMUM_PART_SIZE_MB);
        }
        this.s3 = s3;
        this.bucketName = bucketName;
        this.objectName = objectName;
        this.flushingService = Executors.newFixedThreadPool(numFlushThreads, new ThreadFactoryBuilder().setNameFormat("S3OutputStreamBase-Flusher-%d").build());
        this.circularWriteBuffer = new byte[actualBufferSize];
    }

    protected static long log(String message) {
        long nextMark = System.currentTimeMillis();
        long delta = nextMark - mark;
        System.out.println("(+" + delta + ") " + message);
        mark = nextMark;
        return delta;
    }

    protected abstract byte[] copyToFlushBuffer(byte[] circularBuffer, long startPos, long endPos);

    private synchronized long getCurrentFlushLevel() {
        long minFlushTaskPrevLevel = maxCompletedFlushLevel;
        for (FlushTask task : runningFlushTasks) {
            minFlushTaskPrevLevel = Math.min(minFlushTaskPrevLevel, task.previousFlushLevel);
        }
        return minFlushTaskPrevLevel;
    }

    protected synchronized final void flushAndWait() throws IOException {
        isMultiPart = true;
        final long goal = pos;
        // add a flush task
        flushingService.execute(new FlushTask(true));
        while (getCurrentFlushLevel() < goal) {
            try {
                this.wait();
            } catch (InterruptedException e) {
                // must flush reliably, so retry if there was no exception
            }
            if (flusherException != null) {
                if (flusherException instanceof IOException) {
                    throw (IOException) flusherException;
                }
                throw new IOException(flusherException);
            }
        }
    }

    private final void throwIOExceptionIfClosed() throws IOException {
        if (closed) {
            throw new IOException("write to closed stream not allowed");
        }
    }

    private synchronized void maybeLaunchAFlush() {
        if (pos - previousFlushTarget >= minimumPartSize && !uninitializedFlushSubmitted) {
            isMultiPart = true;
            flushingService.execute(new FlushTask(false));
            uninitializedFlushSubmitted = true;
        }
    }

    @Override
    public void write(int b) throws IOException {
        synchronized (writeOrderMonitor) {
            synchronized (this) {
                maybeLaunchAFlush();
                while (pos - previousFlushTarget >= circularWriteBuffer.length) {
                    throwIOExceptionIfClosed();
                    try {
                        this.wait();
                    } catch (InterruptedException e) {
                        // keep trying
                    }
                }
                throwIOExceptionIfClosed();
                int writePosition = (int) (pos % circularWriteBuffer.length);
                circularWriteBuffer[writePosition] = (byte) (0xff & b);
                pos++;
                maybeLaunchAFlush();
            }
        }
    }

    @Override
    public final void write(byte[] b, int inputOff, int inputLen) throws IOException {
        int off = inputOff;
        int len = inputLen;
        synchronized (writeOrderMonitor) {
            synchronized (this) {
                while (len > 0) {
                    maybeLaunchAFlush();
                    while (pos - previousFlushTarget >= circularWriteBuffer.length) {
                        throwIOExceptionIfClosed();
                        try {
                            this.wait();
                        } catch (InterruptedException e) {
                            // keep trying
                        }
                    }
                    throwIOExceptionIfClosed();
                    int amountToWrite = Math.min(circularWriteBuffer.length - (int) (pos - previousFlushTarget), len);
                    int start = (int) (pos % circularWriteBuffer.length);
                    int end = (int) ((pos + amountToWrite) % circularWriteBuffer.length);
                    if (start < end || end == 0) {
                        System.arraycopy(b, off, circularWriteBuffer, start, amountToWrite);
                    } else {
                        // write past the end
                        int firstCopyLength = circularWriteBuffer.length - start;
                        System.arraycopy(b, off, circularWriteBuffer, start, firstCopyLength);
                        System.arraycopy(b, off + firstCopyLength, circularWriteBuffer, 0, amountToWrite - firstCopyLength);
                    }
                    pos += amountToWrite;
                    off += amountToWrite;
                    len -= amountToWrite;
                }
                maybeLaunchAFlush();
            }
        }
    }

    @Override
    public final void close() throws IOException {
        synchronized (this) {
            closed = true;
            // Only flush and wait if we've started a multipart upload
            if (isMultiPart) {
                flushAndWait();
            }
            flushingService.shutdown();
        }
        while (true) {
            try {
                flushingService.awaitTermination(10, TimeUnit.SECONDS);
                break;
            } catch (InterruptedException e) {
                log.warn("flushing service taking a long time to shut down");
            }
        }

        // we're closed now, so nobody else should mutate state
        if (isMultiPart) {
            // Wrap up the multiPart upload
            if (partETags.isEmpty()) {
                UploadPartResult res = s3.uploadPart(part(1, new byte[0]));
                partETags.add(res.getPartETag());
            }

            CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(bucketName, objectName, uploadId, partETags);
            s3.completeMultipartUpload(completeRequest);
        } else {
            // Because we are closing directly, the circular write buffer can be treated as a plain 'ole linear buffer
            ByteArrayInputStream in = new ByteArrayInputStream(circularWriteBuffer, 0, (int) pos);
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(pos);
            objectMetadata.setContentMD5(Md5Utils.md5AsBase64(new ByteArrayInputStream(circularWriteBuffer, 0, (int) pos)));
            final PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectName, in, objectMetadata);
            long startNanos = System.nanoTime();
            s3.putObject(putObjectRequest);
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            // Only log if took over 3 seconds
            if (log.isTraceEnabled() && elapsedMillis > 3000) {
                S3ResponseMetadata md = s3.getCachedResponseMetadata(putObjectRequest);
                log.trace("Took {}ms to perform putObject: {}", elapsedMillis, md);
            }
        }
    }

    //@Override
    public final void sync() throws IOException {
        flush();
    }

    @Override
    public final void hflush() throws IOException {
        flush();
    }

    @Override
    public final void hsync() throws IOException {
        flush();
    }

    private UploadPartRequest part(int partNumber, byte[] payload) {
        checkState(uploadId != null, "Attempted to generate a part w/o an active multipart upload");
        return new UploadPartRequest()
            .withKey(objectName)
            .withBucketName(bucketName)
            .withUploadId(uploadId)
            .withPartNumber(partNumber)
            .withPartSize(payload.length)
            .withInputStream(new ByteArrayInputStream(payload))
            .withMD5Digest(Md5Utils.md5AsBase64(payload));
    }

    private class FlushTask implements Runnable {

        private final Long flushGoal;
        private final boolean subMinimalFlush;
        // let other threads determine the earliest position that this FlushTask is flushing (to be populated when the FlushTask initializes)
        public long previousFlushLevel = Long.MAX_VALUE;
        private long completeFlushLevel = Long.MIN_VALUE;

        public FlushTask(boolean subMinimalFlush) {
            synchronized (S3OutputStreamBase.this) {
                if (subMinimalFlush) {
                    // don't get too ambitious if another flush has already done the required work (better to fill a buffer)
                    this.flushGoal = pos;
                } else {
                    this.flushGoal = null;
                }

                this.subMinimalFlush = subMinimalFlush;

                // metrics
                S3OutputStreamBase.this.totalFlushTasks++;
            }
        }

        @Override
        public void run() {
            try {
                // initialize the FlushTask
                byte[] toFlush = null;
                long flushSize;
                int partNumber;
                synchronized (S3OutputStreamBase.this) {
                    // allow another FlushTask to be submitted
                    S3OutputStreamBase.this.uninitializedFlushSubmitted = false;
                    // see how much we could flush
                    flushSize = pos - previousFlushTarget;
                    // make sure we either have enough to flush or we're required to flush a smaller buffer
                    if (flushSize < minimumPartSize && (!subMinimalFlush || (flushGoal != null && flushGoal <= previousFlushTarget))) {
                        return;
                    }
                    // keep track of where we will be before and after this flush completes
                    this.previousFlushLevel = S3OutputStreamBase.this.previousFlushTarget;
                    this.completeFlushLevel = pos;
                    // reserve the data that this FlushTask is going to flush
                    toFlush = copyToFlushBuffer(circularWriteBuffer, previousFlushLevel, completeFlushLevel);
                    partNumber = nextPartNumber++;
                    S3OutputStreamBase.this.previousFlushTarget = pos;
                    // allow other threads to discover the earliest position of the buffer that this FlushTask will flush
                    S3OutputStreamBase.this.runningFlushTasks.add(this);
                }
                // do the flush
                if (toFlush != null) {
                    synchronized (flushMonitor) {
                        if (uploadId == null) {
                            InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucketName, objectName);
                            InitiateMultipartUploadResult result = s3.initiateMultipartUpload(request);
                            uploadId = result.getUploadId();
                        }

                        UploadPartResult partResult = s3.uploadPart(part(partNumber, toFlush));
                        partETags.add(new PartETag(partNumber, partResult.getETag()));
                    }
                }
                // clean up
                synchronized (S3OutputStreamBase.this) {
                    S3OutputStreamBase.this.maxCompletedFlushLevel = Math.max(maxCompletedFlushLevel, completeFlushLevel);
                    S3OutputStreamBase.this.runningFlushTasks.remove(this);
                    S3OutputStreamBase.this.notifyAll();

                    // metrics
                    S3OutputStreamBase.this.totalFlushed += flushSize;
                    S3OutputStreamBase.this.totalFlushes++;
                }
            } catch (IOException e) {
                synchronized (S3OutputStreamBase.this) {
                    flusherException = e;
                    closed = true;
                    S3OutputStreamBase.this.notifyAll();
                }
                log.error("Error in FlushTask", e);
            }
        }
    }
}

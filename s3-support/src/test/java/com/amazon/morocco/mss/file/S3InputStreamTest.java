package com.amazon.morocco.mss.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class S3InputStreamTest extends MockS3TestBase {

    public S3InputStreamTest() {
        super("accS3nf");
    }

    @Override
    protected Configuration getConf() {
        Configuration conf = super.getConf();
        conf.setLong(S3InputStream.ExtraBufferAllocationTracker.MAX_EXTRA_BUFFER_TO_ALLOCATE_KEY, 1L << 10);
        conf.setInt(S3InputStream.MIN_BUFFER_SIZE_KEY, 1 << 10);
        conf.setInt(S3InputStream.MAX_BUFFER_SIZE_KEY, 1 << 12);
        return conf;
    }

    @Test
    public void testBufferAllocation() throws Exception {
        FileSystem fs = getFileSystem();
        Path testFile = new Path("/foo");
        try (FSDataOutputStream out = fs.create(testFile)) {
            byte[] buffer = new byte[1 << 14];
            out.write(buffer);
        }

        synchronized (S3InputStream.ExtraBufferAllocationTracker.class) {
            S3InputStream.ExtraBufferAllocationTracker.resetMaxExtraBufferToAllocate();
            assertEquals("Expecting no allocation of extra read buffers yet", 0L, S3InputStream.ExtraBufferAllocationTracker.getCurrentExtraBufferAllocation());
            try (FSDataInputStream in = fs.open(testFile)) {
                byte[] readBuffer = new byte[1 << 9];

                for (int i = 5; i > 0; i--) {
                    // start past the minimum buffer size to avoid triggering the read-from-beginning-of-file buffer increase
                    in.seek(i + 1 << 10);
                    in.readFully(readBuffer);
                    assertEquals(
                        "Expecting allocation to be zero for backwards reads",
                        0L,
                        S3InputStream.ExtraBufferAllocationTracker.getCurrentExtraBufferAllocation());
                }

                in.seek(0);

                // S3InputStream will try to grow the buffer twice, but will only be able to grow it once
                for (int i = 0; i < 5; i++) {
                    in.readFully(readBuffer);
                }
                assertEquals("Expecting allocation to match maximum", 1L << 10, S3InputStream.ExtraBufferAllocationTracker.getCurrentExtraBufferAllocation());

                // test concurrent use of another S3InputStream
                try (FSDataInputStream in2 = fs.open(testFile)) {
                    for (int i = 0; i < 5; i++) {
                        in.readFully(readBuffer);
                        assertEquals(
                            "Expecting allocation to continue to match maximum",
                            1L << 10,
                            S3InputStream.ExtraBufferAllocationTracker.getCurrentExtraBufferAllocation());
                    }
                }

                assertEquals("Expecting allocation to match maximum", 1L << 10, S3InputStream.ExtraBufferAllocationTracker.getCurrentExtraBufferAllocation());
            }
            assertEquals("Expecting buffer allocation to go back to zero", 0L, S3InputStream.ExtraBufferAllocationTracker.getCurrentExtraBufferAllocation());
        }
    }
}

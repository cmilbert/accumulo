package com.amazon.morocco.mss.file;

//import com.amazon.morocco.test.MoroccoTestUtil;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
//import com.amazonaws.services.s3.MockAmazonS3Impl;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.util.Md5Utils;
import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.amazon.morocco.mss.file.S3OutputStreamBase.MINIMUM_BUFFER_SIZE;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class S3NoFlushMultipartOutputStreamTest extends MockS3TestBase {

    private static final String bucket = "bucket";

    public S3NoFlushMultipartOutputStreamTest() {
        super("accS3nf");
    }

    private AmazonS3 mockS3;
    private S3ClientWrapper s3;

    @Before
    public void setup() {
        //mockS3 = spy(new MockAmazonS3Impl());
        s3 = new S3ClientWrapper(mockS3);
        mockS3.createBucket(bucket);
    }

    @Test
    public void testFileReadOperations() throws IOException {
        String object = "object";

        S3NoFlushMultipartOutputStream stream1 = new S3NoFlushMultipartOutputStream(s3, bucket, object, MINIMUM_BUFFER_SIZE);
        stream1.write(1);
        stream1.flusherException = new IOException("Io", new SdkClientException(""));
//        MoroccoTestUtil.expectException(
//            allOf(instanceOf(IOException.class), new CausedByMatcher(SdkClientException.class)),
//            stream1::flushAndWait);
        S3NoFlushMultipartOutputStream stream2 = new S3NoFlushMultipartOutputStream(s3, bucket, object, MINIMUM_BUFFER_SIZE);
        stream2.write(1);
        stream2.flusherException = new IllegalArgumentException("NotIo");
//        MoroccoTestUtil.expectException(
//            allOf(instanceOf(IOException.class), new CausedByMatcher(IllegalArgumentException.class)),
//            stream2::flushAndWait);
    }

    @Test
    public void md5HashesForPutProvided() throws IOException {
        final String object = "object";
        S3NoFlushMultipartOutputStream stream = new S3NoFlushMultipartOutputStream(s3, bucket, object, MINIMUM_BUFFER_SIZE);
        stream.write(1);
        stream.close();

        final String expectedMd5 = Md5Utils.md5AsBase64(new byte[] { 1 });
        verify(mockS3, times(1)).putObject(argThat(request -> expectedMd5.equals(request.getMetadata().getContentMD5())));
    }

    @Test
    public void md5HashesForUploadPartProvided() throws IOException {
        final String object = "object";
        S3NoFlushMultipartOutputStream stream = new S3NoFlushMultipartOutputStream(s3, bucket, object, MINIMUM_BUFFER_SIZE);
        final byte[] content = Strings.repeat("a", MINIMUM_BUFFER_SIZE).getBytes(StandardCharsets.UTF_8);
        stream.write(content);
        stream.close();

        final String expectedMd5 = Md5Utils.md5AsBase64(content);
        verify(mockS3, times(1)).uploadPart(argThat(request -> expectedMd5.equals(request.getMd5Digest())));
    }

    @Test
    public void errorInFlushDoesNotTriggerUncaughtExceptionHandler() throws InterruptedException {
        AtomicBoolean triggered = new AtomicBoolean(false);
        Thread.UncaughtExceptionHandler old = Thread.getDefaultUncaughtExceptionHandler();
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> triggered.set(true));

        try {
            String object = "object";
            S3NoFlushMultipartOutputStream stream = new S3NoFlushMultipartOutputStream(s3, bucket, object, MINIMUM_BUFFER_SIZE);
            doThrow(new AmazonS3Exception("injected")).when(mockS3).uploadPart(any());

//            MoroccoTestUtil.expectException(new CausedByMatcher(AmazonS3Exception.class), () -> {
//                stream.write(Strings.repeat("A", MINIMUM_BUFFER_SIZE).getBytes(StandardCharsets.UTF_8));
//                stream.close();
//            });

            Thread.sleep(10); // Yield a little to give the uncaught exception handler a chance to trigger
            assertThat(triggered.get(), is(false));
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(old);
        }
    }

    @AllArgsConstructor
    public static class CausedByMatcher extends BaseMatcher<Exception> {

        private Class<? extends Throwable> causedby;

        @Override
        public boolean matches(Object item) {
            if (item instanceof Exception) {
                return ((Exception) item).getCause().getClass().equals(causedby);
            }
            return false;
        }

        @Override
        public void describeTo(Description description) {

        }
    }
}

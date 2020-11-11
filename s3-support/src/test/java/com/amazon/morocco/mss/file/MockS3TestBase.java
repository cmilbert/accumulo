package com.amazon.morocco.mss.file;

import com.amazonaws.services.s3.AmazonS3;
//import com.amazonaws.services.s3.MockAmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


import static com.amazon.morocco.mss.file.AccumuloS3FileSystemBase.USE_STATIC_CLIENT;

public class MockS3TestBase {

    protected final String bucketName;
    protected final String scheme;

    protected AmazonS3 s3;

    public MockS3TestBase(String scheme) {
        bucketName = getClass().getSimpleName();
        this.scheme = scheme;
    }

    protected FileSystem getFileSystem() {
        return getFileSystemForSchem(scheme);
    }

    protected FileSystem getFileSystemForSchem(String scheme) {
        try {
            final URI uri = new URI(scheme + "://" + bucketName);
            final Configuration conf = getConf();
            AccumuloS3FileSystemBase.staticClient = s3;
            return FileSystem.get(uri, conf);
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void setupMockS3() {
        //s3 = MockAmazonS3.newInstance();
        CreateBucketRequest req = new CreateBucketRequest(bucketName);
        s3.createBucket(req);
    }

    @After
    public void cleanupMockS3() throws IOException {
        try {
            s3.deleteBucket(bucketName);
        } catch (AmazonS3Exception e) {
            // some tests clobber this, so ignore if it happens
        }
        FileSystem.closeAll();
    }

    protected Configuration getConf() {
        final Configuration configuration = new Configuration();
        configuration.setBoolean(USE_STATIC_CLIENT, true);

        return configuration;
    }
}

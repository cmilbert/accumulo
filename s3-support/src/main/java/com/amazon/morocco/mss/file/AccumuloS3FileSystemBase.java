package com.amazon.morocco.mss.file;

//import com.amazon.morocco.util.java.s3.MoroccoS3ClientBuilder;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonServiceException.ErrorType;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PartSummary;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Slf4j
public abstract class AccumuloS3FileSystemBase extends FileSystem {

    private static final int S3_CONNECTION_TIMEOUT_MS = 1000;
    private static final int S3_REQUEST_TIMEOUT_MS = 30000;
    private static final int S3_SOCKET_TIMEOUT_MS = 30000;
    private static final int S3_CLIENT_EXECUTION_TIMEOUT_MS = 30000;
    protected static final String DIR_MARKER = "__$DIR$";
    public static final String USE_STATIC_CLIENT = "com.amazon.morocco.mss.file.useStaticClient";

    @VisibleForTesting
    @SuppressFBWarnings("MS_SHOULD_BE_FINAL")
    public static AmazonS3 staticClient = null;

    private static long blockSize = 1 << 30;
    protected String bucketName = null;
    protected S3ClientWrapper s3 = null;
    protected Statistics stats = null;
    private Path workingDirectory = null;
    private URI uri;

    public AccumuloS3FileSystemBase() {
        super();
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        AmazonS3 s3Client;
        if (conf.getBoolean(USE_STATIC_CLIENT, false)) {
            s3Client = staticClient;
        } else {
            s3Client = constructS3Client();
        }

        initialize(name, conf, s3Client);
    }

    public void initialize(URI name, Configuration conf, AmazonS3 s3Client) throws IOException {
        setConf(conf);
        log.trace("initializing with URI {}", name);
        super.initialize(name, conf);

        s3 = new S3ClientWrapper(s3Client);

        this.uri = URI.create(name.getScheme() + "://" + name.getAuthority());

        bucketName = name.getAuthority();

        String pathName = name.getPath();
        if (!pathName.isEmpty()) {
            workingDirectory = new Path(name.getPath());
        } else {
            workingDirectory = new Path("/");
        }
    }

    private AmazonS3 constructS3Client() {
        AmazonS3ClientBuilder builder = S3ClientBuilder
            .builder(getS3Credentials(), null)
            .withClientConfiguration(new ClientConfiguration()
                .withConnectionTimeout(S3_CONNECTION_TIMEOUT_MS)
                .withRequestTimeout(S3_REQUEST_TIMEOUT_MS)
                .withSocketTimeout(S3_SOCKET_TIMEOUT_MS)
                .withClientExecutionTimeout(S3_CLIENT_EXECUTION_TIMEOUT_MS));

        return builder.build();
    }

    protected AWSCredentialsProvider getS3Credentials() {
        return DefaultAWSCredentialsProviderChain.getInstance();
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public FSDataOutputStream append(Path arg0, int arg1, Progressable arg2) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean rename(Path oldName, Path newName) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        log.trace("getFileStatus for {}", path);
        ObjectMetadata meta = s3.getObjectMetadata(bucketName, getObjectName(path));
        if (meta != null) {
            Date lastModifiedDate = meta.getLastModified();
            if (lastModifiedDate == null) {
                lastModifiedDate = new Date(0);
            }
            return new FileStatus(meta.getContentLength(), false, 1, blockSize, lastModifiedDate.getTime(), 0, null, null, null, path);
        }
        ObjectListing listing = s3.listObjects(bucketName, getDirName(path));
        if (!listing.getObjectSummaries().isEmpty()) {
            // this is a directory
            return new FileStatus(0, true, 0, 0, 0, 0, null, null, null, new Path(getScheme() + "://" + bucketName + "/" + getObjectName(path)));
        }
        throw new FileNotFoundException();
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        log.trace("deleting {}, recursive={}", path, recursive);
        String objectName = getObjectName(path);
        if (recursive) {
            String marker = null;
            ListObjectsRequest listReq = new ListObjectsRequest(bucketName, objectName + "/", marker, null, 1000);
            while (true) {
                listReq.withMarker(marker);
                ObjectListing listing = s3.listObjects(listReq);
                List<KeyVersion> keys = new ArrayList<>();
                for (S3ObjectSummary summary : listing.getObjectSummaries()) {
                    keys.add(new KeyVersion(summary.getKey()));
                }
                if (!keys.isEmpty()) {
                    s3.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(keys));
                }
                if (listing.isTruncated()) {
                    marker = listing.getMarker();
                } else {
                    break;
                }
            }
            return true;
        } else {
            return s3.deleteObject(bucketName, objectName);
        }
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucketName, getDirName(path), null, "/", 1024);
        ObjectListing listing;
        String marker = null;
        ArrayList<FileStatus> statuses = new ArrayList<>();
        do {
            listing = s3.listObjects(listObjectsRequest.withMarker(marker));
            for (S3ObjectSummary obj : listing.getObjectSummaries()) {
                if (obj.getKey().endsWith("/" + DIR_MARKER)) {
                    // skip the special object used to mark empty directories
                    continue;
                }
                Date lastModifiedDate = obj.getLastModified();
                if (lastModifiedDate == null) {
                    lastModifiedDate = new Date(0);
                }
                statuses.add(new FileStatus(obj.getSize(), false, 1, blockSize, lastModifiedDate.getTime(), 0, null, null, null,
                    new Path(getScheme() + "://" + bucketName + "/" + obj.getKey())));
            }
            for (String prefix : listing.getCommonPrefixes()) {
                statuses.add(new FileStatus(0, true, 0, 0, 0, 0, null, null, null, new Path(getScheme() + "://" + bucketName + "/" + prefix)));
            }
            marker = listing.getNextMarker();
        } while (listing.isTruncated());
        return statuses.toArray(new FileStatus[statuses.size()]);
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDirectory;
    }

    protected String getDirName(Path path) {
        String objectName = getObjectName(path);
        if (objectName.isEmpty()) {
            // object keys don't start with "/", so we only put that in if we've specified something other than the root
            return objectName;
        } else {
            return objectName + "/";
        }
    }

    protected String getObjectName(Path inputPath) {
        Path path = inputPath;
        if (!path.isAbsolute()) {
            path = new Path(workingDirectory, path);
        }
        if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
            // allow uris without trailing slash after bucket to refer to root,
            // like s3n://mybucket
            return "";
        }
        if (!path.isAbsolute()) {
            throw new IllegalArgumentException("Invalid path - must be absolute: " + path);
        }
        String ret = path.toUri().getPath().substring(1); // remove initial slash
        if (ret.endsWith("/") && (ret.indexOf("/") != ret.length() - 1)) {
            ret = ret.substring(0, ret.length() - 1);
        }
        return ret;
    }

    @Override
    public boolean mkdirs(Path path, FsPermission perm) throws IOException {
        if (exists(path)) {
            return false;
        }
        createNewFile(new Path(path, DIR_MARKER));
        return true;
    }

    private void closeMultipartUpload(String objectName, String uploadId) throws IOException {
        // keep trying until it's closed
        ListPartsRequest req = new ListPartsRequest(bucketName, objectName, uploadId);
        List<PartETag> partETags = new ArrayList<PartETag>();
        // S3 will truncate listings of over 1,000 parts, so we need to page through them
        for (PartListing l = s3.listParts(req); l != null; l = l.isTruncated() ? s3.listParts(req.withPartNumberMarker(l.getNextPartNumberMarker())) : null) {
            for (PartSummary summary : l.getParts()) {
                partETags.add(new PartETag(summary.getPartNumber(), summary.getETag()));
            }
        }

        if (partETags.isEmpty()) {
            s3.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, objectName, uploadId));
        } else {
            CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(bucketName, objectName, uploadId, partETags);
            s3.completeMultipartUpload(completeRequest);
        }
    }

    // TODO: make this cancelable or otherwise limited in its retries
    // used to recover from a tserver failure. should block until no more writes to WAL are possible
    public void closeMultipartFile(Path path, long retryTimeoutMillis, long giveUpTimeoutMillis) throws IOException {
        String objectName = getObjectName(path);
        long startTime = System.currentTimeMillis();
        while (true) {
            // keep trying until success or unrecoverable failure
            try {
                String uploadId = null;

                ListMultipartUploadsRequest baseRequest = new ListMultipartUploadsRequest(bucketName).withPrefix(objectName);
                MultipartUploadListing listing = s3.listMultipartUploads(baseRequest);
                while (true) {
                    for (MultipartUpload upload : listing.getMultipartUploads()) {
                        if (upload.getKey().equals(objectName)) {
                            if (uploadId != null) {
                                log.warn("multiple upload sessions found for WAL {}", path);
                            }
                            uploadId = upload.getUploadId();
                            closeMultipartUpload(objectName, uploadId);
                        }
                    }
                    if (listing.isTruncated()) {
                        listing = s3.listMultipartUploads(baseRequest
                            .withKeyMarker(listing.getNextKeyMarker())
                            .withUploadIdMarker(listing.getNextUploadIdMarker()));
                    } else {
                        break;
                    }
                }
                if (uploadId == null) {
                    // no upload found for given object, so it might already be closed?
                    if (exists(path)) {
                        return;
                    }
                    // failed to find the upload ID and failed to find the completed file, so try again
                }
                long delta = System.currentTimeMillis() - startTime;
                if (giveUpTimeoutMillis > 0 && delta > giveUpTimeoutMillis) {
                    throw new IOException("giving up on closing WAL");
                }
                try {
                    Thread.sleep(retryTimeoutMillis);
                } catch (InterruptedException e) {
                    // just try again
                }
                // TODO: check state, like process trying to shut down?
            } catch (AmazonServiceException e) {
                if (e.getErrorType().equals(ErrorType.Service)) {
                    // TODO: validate that this is a failure to close due to some parameter issue, such as the original server uploaded another part since the
                    // part listing; try again
                    assert true;
                } else {
                    // don't know how to handle this
                    throw new IOException(e);
                }
            }
        }
    }

    @Override
    public final void setWorkingDirectory(Path path) {
        this.workingDirectory = path;
    }



}

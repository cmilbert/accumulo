package com.amazon.morocco.mss.file;

import com.google.auto.service.AutoService;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;

@AutoService(FileSystem.class)
public class AccumuloMultiObjectS3FileSystem extends AccumuloS3FileSystemBase {

    public static final String scheme = "accS3mo";
    public static final String partPrefix = "$PART_";

    public AccumuloMultiObjectS3FileSystem() {
    }

    @Override
    public String getScheme() {
        return scheme;
    }

    @Override
    public FSDataOutputStream create(
        Path path,
        FsPermission fsPermission,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progressable) throws IOException {

        // ignore bufferSize, replication, and blockSize
        String objectName = getObjectName(path);
        // use the noflush output stream for dir markers so that we don't collapse dir markers on read
        if (path.getName().equals(DIR_MARKER)) {
            return new FSDataOutputStream(new S3NoFlushMultipartOutputStream(s3, bucketName, objectName, bufferSize), stats);
        } else {
            return new FSDataOutputStream(new S3MultiObjectOutputStream(s3, bucketName, objectName), stats);
        }
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        String objectName = getObjectName(path);
        return new FSDataInputStream(new S3MultiObjectInputStream(s3, bucketName, objectName));
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        // always recurse because files are multiple objects
        return super.delete(path, true);
    }

    private FileStatus collapseStatus(FileStatus fs) throws FileNotFoundException, IOException {
        if (fs.isDirectory()) {
            FileStatus[] children = super.listStatus(fs.getPath());
            if (children.length == 0) {
                return fs;
            }
            long totalSize = 0;
            long lastModTime = 0;
            for (FileStatus child : children) {
                if (!child.getPath().getName().startsWith(partPrefix)) {
                    return fs;
                } else {
                    totalSize += child.getLen();
                    lastModTime = Math.max(lastModTime, child.getModificationTime());
                }
            }
            return new FileStatus(totalSize, false, 1, 0, lastModTime, 0, null, null, null, fs.getPath());
        } else {
            return fs;
        }
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        FileStatus[] superStatuses = super.listStatus(path);
        for (int i = 0; i < superStatuses.length; i++) {
            superStatuses[i] = collapseStatus(superStatuses[i]);
        }
        return superStatuses;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        FileStatus fs = super.getFileStatus(path);
        return collapseStatus(fs);
    }
}

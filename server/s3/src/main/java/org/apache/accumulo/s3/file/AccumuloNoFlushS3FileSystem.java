package org.apache.accumulo.s3.file;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import com.google.auto.service.AutoService;

@AutoService(FileSystem.class)
public class AccumuloNoFlushS3FileSystem extends AccumuloS3FileSystemBase {

  public static final String scheme = "accS3nf";

  public AccumuloNoFlushS3FileSystem() {}

  @Override
  public String getScheme() {
    return scheme;
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    String objectName = getObjectName(path);
    return new FSDataInputStream(new S3InputStream(s3, bucketName, objectName, getConf()));
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progressable)
      throws IOException {
    // ignore replication and blockSize
    // TODO: do something with permissions
    // TODO: something with the Progressable
    // TODO: handle overwrite
    String objectName = getObjectName(path);
    return new FSDataOutputStream(
        new S3NoFlushMultipartOutputStream(s3, bucketName, objectName, bufferSize), stats);
  }
}

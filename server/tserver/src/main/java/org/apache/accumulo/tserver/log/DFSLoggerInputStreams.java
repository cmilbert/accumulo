package org.apache.accumulo.tserver.log;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.DataInputStream;

public class DFSLoggerInputStreams {

  private FSDataInputStream originalInput;
  private DataInputStream decryptingInputStream;

  public DFSLoggerInputStreams(FSDataInputStream originalInput,
                               DataInputStream decryptingInputStream) {
    this.originalInput = originalInput;
    this.decryptingInputStream = decryptingInputStream;
  }

  public FSDataInputStream getOriginalInput() {
    return originalInput;
  }

  public DataInputStream getDecryptingInputStream() {
    return decryptingInputStream;
  }
}

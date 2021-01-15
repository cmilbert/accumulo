package org.apache.accumulo.tserver.log;

import java.io.DataInputStream;

import org.apache.hadoop.fs.FSDataInputStream;

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

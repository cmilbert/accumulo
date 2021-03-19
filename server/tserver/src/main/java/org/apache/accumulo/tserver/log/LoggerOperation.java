package org.apache.accumulo.tserver.log;

import java.io.IOException;

class LoggerOperation {
  private final LogWork work;

  public LoggerOperation(LogWork work) {
    this.work = work;
  }

  public void await() throws IOException {
    try {
      work.latch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    if (work.exception != null) {
      if (work.exception instanceof IOException)
        throw (IOException) work.exception;
      else if (work.exception instanceof RuntimeException)
        throw (RuntimeException) work.exception;
      else
        throw new RuntimeException(work.exception);
    }
  }
}

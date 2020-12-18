package org.apache.accumulo.tserver.log;

class NoWaitLoggerOperation extends LoggerOperation {

  public NoWaitLoggerOperation() {
    super(null);
  }

  @Override
  public void await() {
    return;
  }
}

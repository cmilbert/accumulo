package org.apache.accumulo.fate.zookeeper;

import org.apache.zookeeper.KeeperException;

public interface ServerLease {
  long getSessionId() throws KeeperException, InterruptedException;

  boolean wasLockAcquired();

  boolean isLocked();

  ZooUtil.LockID getLockID();
  // String getLockPath();
  // String getLockName();
}

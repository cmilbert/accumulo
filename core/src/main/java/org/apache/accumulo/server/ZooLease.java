package org.apache.accumulo.server.zookeeper;

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooLease implements Closeable, ServerLease {

  private static final Logger log = LoggerFactory.getLogger(ZooLease.class);
  private final String path;
  private final String child;
  private final long leaseTimeMillis;
  private final ZooLock.LockWatcher lockWatcher;
  private ZooKeeper zk;
  private boolean leaseWasAcquired = false;
  private boolean closed = false;
  private Long lastSyncTime = null;
  private Timer leaseSyncTimer = new Timer();
  private boolean leaseSyncScheduled = false;
  private boolean syncInProgress = false;
  private boolean initialized = false;

  private ZooLease(ZooKeeper zk, String path, String child, long leaseTimeMillis,
      ZooLock.LockWatcher lockWatcher) {
    this.zk = zk;
    this.path = path;
    this.child = child;
    this.leaseTimeMillis = leaseTimeMillis;
    this.lockWatcher = lockWatcher;
    checkAndMonitorLockState();
  }

  /**
   * Requesting a lease is tantamount to creating an ephemeral sequential node under the lease root,
   * so this method will create and return a ZooLease once that ephemeral sequential node has been
   * created
   *
   * @param zk
   *          A valid ZooKeeper client that can read and write to the given path
   * @param path
   *          The path in ZK that must already have been created and must be writable by the given
   *          zk client
   * @param data
   *          User specified data that will be stored in the lease node
   * @param acls
   *          Access control list to be used in creating the lease node
   * @param lockWatcher
   *          A callback that will be notified that the lease has been lost only after it has been
   *          acquired
   * @return A handle for checking on and controlling the lease
   * @throws IOException
   *           when there is a problem creating the lease node
   */
  public static ZooLease requestLease(ZooKeeper zk, String path, byte[] data, List<ACL> acls,
      ZooLock.LockWatcher lockWatcher) throws IOException {
    try {
      // use the same lock prefix as ZooLock so that use of ZooLease will be compatible with use of
      // ZooLock
      String childNode =
          zk.create(path + "/" + ZooLock.LOCK_PREFIX, data, acls, CreateMode.EPHEMERAL_SEQUENTIAL);
      // get rid of the path, we just want the child name
      childNode = childNode.substring(path.length() + 1);
      ZooLease lease = new ZooLease(zk, path, childNode, zk.getSessionTimeout(), lockWatcher);
      // wait for the least to be initialized before returning
      lease.waitForInitialization();
      return lease;
    } catch (Exception e) {
      log.error("failed to create lease child due to unknown exception", e);
      throw new IOException("problem when attempting to create lease child", e);
    }
  }

  /**
   * Lease is held if (1) we think we're first in line, (2) our ZooKeeper client is connected, and
   * (3) we have recently synced. Lease is not held if (1) we think another ZooLease is first in
   * line, (2) our ZooKeeper client is closed, (3) our lease node has been deleted. Lease may or may
   * not be held otherwise, and this method will block until one of those conditions is met.
   *
   * @return true if and only if this ZooLease has observed the lease was held at some time between
   *         when this method was called and when it returned
   */
  public synchronized boolean isLeaseHeld() {
    if (closed || !leaseWasAcquired || zkInBadState()) {
      return false;
    }

    // make sure we've synchronized recently
    while (lastSyncTime == null || System.currentTimeMillis() > lastSyncTime + leaseTimeMillis) {
      try {
        this.wait();
      } catch (InterruptedException e) {
        // do nothing special
      }

      if (closed || zkInBadState()) {
        return false;
      }
    }
    return true;
  }

  public synchronized boolean isLocked() {
    return this.isLeaseHeld();
  }

  /**
   * @return Whether this ZooLease object observed that the lease was acquired at some point in the
   *         past
   */
  public synchronized boolean wasLockAcquired() {
    return leaseWasAcquired;
  }

  /**
   * Delete the lease node in ZooKeeper and notify watchers if the lease was previously held
   */
  public void unlock() {
    // note the close locally before we delete the node in ZK so that we don't
    // let another thread and another server both think they hold the lease at the same time
    closeAndNotify(ZooLock.LockLossReason.LOCK_DELETED);
    final String pathToDelete = path + "/" + child;
    AsyncCallback.StatCallback cb = new AsyncCallback.StatCallback() {

      @Override
      public void processResult(int rc, String path, Object ctx, Stat stat) {
        if (rc == KeeperException.Code.NONODE.intValue()
            || stat == null) {} else if (rc == KeeperException.Code.OK.intValue()) {
          zk.delete(pathToDelete, stat.getVersion(), (rc2, path2, ctx2) -> {
            if (rc2 == KeeperException.Code.OK.intValue()
                || rc2 == KeeperException.Code.NONODE.intValue()) {} else if (rc2
                    == KeeperException.Code.BADVERSION.intValue()) {
              zk.exists(pathToDelete, false, this, null);
            } else {
              log.warn("Problem deleting lease node {}", KeeperException.Code.get(rc2));
            }
          }, null);
        } else {
          log.warn("Problem deleting lease node {}", KeeperException.Code.get(rc));
        }
      }
    };
    zk.exists(pathToDelete, false, cb, null);
  }

  @Override
  public void close() {
    boolean needToUnlock = false;
    synchronized (ZooLease.this) {
      if (!closed) {
        log.warn("ZooLease closed before unlock called");
        needToUnlock = true;
      }
    }
    if (needToUnlock) {
      unlock();
    }
  }

  /**
   * @return a LockID object that can serialize information needed to check on the state of this
   *         lease
   */
  public ZooUtil.LockID getLockID() {
    return new ZooUtil.LockID(path, child, zk.getSessionId());
  }

  /**
   * @return The session ID for the ZooKeeper client used by this ZooLease, also the ephemeral ID
   *         for the lease node
   */
  public long getSessionId() {
    return zk.getSessionId();
  }

  private synchronized boolean zkInBadState() {
    // check for conditions that signal we definitely don't hold the lease anymore, like something
    // closed the zk connection
    switch (zk.getState()) {
      case CLOSED:
      case AUTH_FAILED:
        // it's not possible to hold a lease with these states
        closeAndNotify(ZooLock.LockLossReason.SESSION_EXPIRED);
        return true;
      case CONNECTING:
      case ASSOCIATING:
      case CONNECTED:
      case CONNECTEDREADONLY:
      case NOT_CONNECTED:
      default:
        // it may be possible to hold a lease with these states
        return false;
    }
  }

  private synchronized void closeAndNotify(ZooLock.LockLossReason reason) {
    if (!closed) {
      closed = true;
      if (leaseSyncTimer != null) {
        leaseSyncTimer.cancel();
        leaseSyncTimer = null;
      }
      this.notifyAll();
      if (leaseWasAcquired && lockWatcher != null) {
        // notify in a new thread to avoid possible deadlocks
        new Thread(() -> lockWatcher.lostLock(reason)).start();
      }
    }
  }

  private synchronized void checkAndMonitorLockState() {
    if (closed) {
      return;
    }

    if (zkInBadState()) {
      return;
    }

    // get the list of all clients that have tried to get the lease
    zk.getChildren(path, false, (getChildrenReturnCode, path, getChildrenContext, children) -> {
      if (getChildrenReturnCode == KeeperException.Code.CONNECTIONLOSS.intValue()) {
        checkAndMonitorLockState();
        return;
      } else if (getChildrenReturnCode != KeeperException.Code.OK.intValue()) {
        log.warn("getChildren failed: {}. Unlocking.",
            KeeperException.Code.get(getChildrenReturnCode));
        unlock();
        return;
      }

      if (!children.contains(child)) {
        closeAndNotify(ZooLock.LockLossReason.LOCK_DELETED);
        log.info("Lease check attempt ephemeral node no longer exist {}/{}", path, child);
        return;
      }
      // grab the child with the minimum ID, i.e. the one that holds the lease
      String minChild = children.stream().min(Comparator.naturalOrder()).get();
      String pathToWatch = path + "/" + minChild;
      synchronized (ZooLease.this) {
        if (!leaseWasAcquired && minChild.equals(child)) {
          leaseWasAcquired = true;
          startSyncCheck();
        }
      }

      final long statSyncTime = System.currentTimeMillis();

      // atomically verify that the first lease node exists and that we want to run this method
      // again if anything changes
      zk.exists(pathToWatch, watchedEvent -> {
        if (watchedEvent.getState() != null) {
          switch (watchedEvent.getState()) {
            case Expired:
            case AuthFailed:
              closeAndNotify(ZooLock.LockLossReason.SESSION_EXPIRED);
              return;
            default:
              break;
          }
        }
        checkAndMonitorLockState();
      }, (statReturnCode, statPath, statContext, stat) -> {
        synchronized (ZooLease.this) {
          if (!initialized) {
            initialized = true;
            this.notifyAll();
          }
          if (statReturnCode == KeeperException.Code.NONODE.intValue() || stat == null) {
            // the node we tried to stat doesn't exist, indicating either our lease was deleted or
            // the node holding the lease (not ours) was deleted
            if (leaseWasAcquired) {
              closeAndNotify(ZooLock.LockLossReason.LOCK_DELETED);
            } else {
              checkAndMonitorLockState();
            }
            return;
          } else if (statReturnCode == KeeperException.Code.CONNECTIONLOSS.intValue()) {
            checkAndMonitorLockState();
          } else if (statReturnCode != KeeperException.Code.OK.intValue()) {
            log.warn("stat of lease node failed: {}. Unlocking.",
                KeeperException.Code.get(statReturnCode));
            unlock();
            return;
          }
          // stat does a round trip, so we can establish a sync time
          lastSyncTime = lastSyncTime == null ? statSyncTime : Math.max(statSyncTime, lastSyncTime);
        }
      }, statSyncTime);
    }, null);
  }

  private synchronized void waitForInitialization() {
    while (!closed && !initialized) {
      try {
        this.wait();
      } catch (InterruptedException e) {
        // do nothing
      }
    }
  }

  private synchronized void scheduleSyncCheck() {
    // only allow one future lease check to be scheduled at a time
    if (leaseSyncTimer != null) {
      leaseSyncTimer.schedule(new TimerTask() {

        @Override
        public void run() {
          startSyncCheck();
        }
      }, leaseTimeMillis / 4);
    }
  }

  private synchronized void startSyncCheck() {
    // only run sync checks if we think we hold the lease
    if (leaseWasAcquired && !closed) {
      if (!syncInProgress) {
        Long currentTime = System.currentTimeMillis();
        zk.sync(path, (returnCode, path, context) -> {
          synchronized (ZooLease.this) {
            syncInProgress = false;
            if (returnCode != KeeperException.Code.OK.intValue()) {
              if (returnCode == KeeperException.Code.CONNECTIONLOSS.intValue()) {
                // ZooKeeper can disconnect without expiring the session, but CONNECTIONLOSS return
                // code is used for both cases
                if (zkInBadState()) {
                  closeAndNotify(ZooLock.LockLossReason.SESSION_EXPIRED);
                  return;
                } else {
                  scheduleSyncCheck();
                  return;
                }
              }
              log.warn("Sync failed: {}. Unlocking.", KeeperException.Code.get(returnCode));
              unlock();
              return;
            }
            // always override the previous sync time in case the system clock changed
            lastSyncTime = (Long) context;
            // other threads may be waiting for the sync time to be updated
            ZooLease.this.notifyAll();

            scheduleSyncCheck();
          }
        }, currentTime);
        syncInProgress = true;
      }
    }
  }
}

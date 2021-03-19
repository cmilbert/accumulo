package org.apache.accumulo.tserver.log;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;

public interface ServerResources {
  AccumuloConfiguration getConfiguration();

  VolumeManager getFileSystem();
}

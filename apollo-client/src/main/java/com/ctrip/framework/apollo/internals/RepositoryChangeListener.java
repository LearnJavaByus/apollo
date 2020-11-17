package com.ctrip.framework.apollo.internals;

import java.util.Properties;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public interface RepositoryChangeListener {
  /**
   * Invoked when config repository changes.  配置变化操作
   * @param namespace the namespace of this repository change
   * @param newProperties the properties after change
   */
  public void onRepositoryChange(String namespace, Properties newProperties);
}

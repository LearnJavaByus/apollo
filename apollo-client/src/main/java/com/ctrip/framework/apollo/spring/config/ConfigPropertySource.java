package com.ctrip.framework.apollo.spring.config;

import com.ctrip.framework.apollo.ConfigChangeListener;
import java.util.Set;

import org.springframework.core.env.EnumerablePropertySource;

import com.ctrip.framework.apollo.Config;

/**
 * Property source wrapper for Config  基于 Apollo Config 的 PropertySource 实现类
 *
 * @author Jason Song(song_s@ctrip.com)
 */
public class ConfigPropertySource extends EnumerablePropertySource<Config> {
  private static final String[] EMPTY_ARRAY = new String[0];
  // // 此处的 Apollo Config 作为 `source
  ConfigPropertySource(String name, Config source) {
    super(name, source);
  }

  @Override
  public String[] getPropertyNames() {
    // 从 Config 中，获得属性名集合
    Set<String> propertyNames = this.source.getPropertyNames();
    // 转换成 String 数组，返回
    if (propertyNames.isEmpty()) {
      return EMPTY_ARRAY;
    }
    return propertyNames.toArray(new String[propertyNames.size()]);
  }

  @Override
  public Object getProperty(String name) {
    return this.source.getProperty(name, null);
  }
  /**
   * 添加 ConfigChangeListener 到 Config 中
   *
   * @param listener 监听器
   */
  public void addChangeListener(ConfigChangeListener listener) {
    this.source.addChangeListener(listener);
  }
}

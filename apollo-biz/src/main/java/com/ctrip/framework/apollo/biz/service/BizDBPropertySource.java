package com.ctrip.framework.apollo.biz.service;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import com.ctrip.framework.apollo.biz.entity.ServerConfig;
import com.ctrip.framework.apollo.biz.repository.ServerConfigRepository;
import com.ctrip.framework.apollo.common.config.RefreshablePropertySource;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.foundation.Foundation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;

/**
 * @author Jason Song(song_s@ctrip.com)
 *
 * 实现 RefreshablePropertySource 抽象类，基于 ConfigDB 的 ServerConfig 的 PropertySource 实现类
 */
@Component
public class BizDBPropertySource extends RefreshablePropertySource {

  private static final Logger logger = LoggerFactory.getLogger(BizDBPropertySource.class);

  @Autowired
  private ServerConfigRepository serverConfigRepository;

  public BizDBPropertySource(String name, Map<String, Object> source) {
    super(name, source);
  }

  public BizDBPropertySource() {
    super("DBConfig", Maps.newConcurrentMap());
  }

  String getCurrentDataCenter() {
    return Foundation.server().getDataCenter();
  }

  @Override
  protected void refresh() {
    // 获得所有的 ServerConfig 记录
    Iterable<ServerConfig> dbConfigs = serverConfigRepository.findAll();
    // 创建配置 Map ，将匹配的 Cluster 的 ServerConfig 添加到其中
    Map<String, Object> newConfigs = Maps.newHashMap();
    //default cluster's configs  // 匹配默认的 Cluster
    for (ServerConfig config : dbConfigs) {
      if (Objects.equals(ConfigConsts.CLUSTER_NAME_DEFAULT, config.getCluster())) {
        newConfigs.put(config.getKey(), config.getValue());
      }
    }

    //data center's configs // 匹配数据中心的 Cluster
    String dataCenter = getCurrentDataCenter();
    for (ServerConfig config : dbConfigs) {
      if (Objects.equals(dataCenter, config.getCluster())) {
        newConfigs.put(config.getKey(), config.getValue());
      }
    }

    //cluster's config // 匹配 JVM 启动参数的 Cluster
    if (!Strings.isNullOrEmpty(System.getProperty(ConfigConsts.APOLLO_CLUSTER_KEY))) {
      String cluster = System.getProperty(ConfigConsts.APOLLO_CLUSTER_KEY);
      for (ServerConfig config : dbConfigs) {
        if (Objects.equals(cluster, config.getCluster())) {
          newConfigs.put(config.getKey(), config.getValue());
        }
      }
    }

    //put to environment // 缓存，更新到属性源
    for (Map.Entry<String, Object> config: newConfigs.entrySet()){
      String key = config.getKey();
      Object value = config.getValue();

      if (this.source.get(key) == null) {
        logger.info("Load config from DB : {} = {}", key, value);
      } else if (!Objects.equals(this.source.get(key), value)) {
        logger.info("Load config from DB : {} = {}. Old value = {}", key,
                    value, this.source.get(key));
      }
      // 更新到属性源
      this.source.put(key, value);

    }

  }

}

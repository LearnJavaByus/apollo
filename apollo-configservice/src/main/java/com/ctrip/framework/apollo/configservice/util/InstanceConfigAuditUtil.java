package com.ctrip.framework.apollo.configservice.util;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;

import com.ctrip.framework.apollo.biz.entity.Instance;
import com.ctrip.framework.apollo.biz.entity.InstanceConfig;
import com.ctrip.framework.apollo.biz.service.InstanceService;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.tracer.Tracer;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Service
public class InstanceConfigAuditUtil implements InitializingBean {
  /**
   * {@link #audits} 大小
   */
  private static final int INSTANCE_CONFIG_AUDIT_MAX_SIZE = 10000;
  /**
   * {@link #instanceCache} 大小
   */
  private static final int INSTANCE_CACHE_MAX_SIZE = 50000;
  /**
   * {@link #instanceConfigReleaseKeyCache} 大小
   */
  private static final int INSTANCE_CONFIG_CACHE_MAX_SIZE = 50000;
  private static final long OFFER_TIME_LAST_MODIFIED_TIME_THRESHOLD_IN_MILLI = TimeUnit.MINUTES.toMillis(10);//10 minutes
  private static final Joiner STRING_JOINER = Joiner.on(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR);
  /**
   * ExecutorService 对象。队列大小为 1 。
   */
  private final ExecutorService auditExecutorService;
  /**
   * 是否停止
   */
  private final AtomicBoolean auditStopped;
  /**
   * 队列
   */
  private BlockingQueue<InstanceConfigAuditModel> audits = Queues.newLinkedBlockingQueue
      (INSTANCE_CONFIG_AUDIT_MAX_SIZE);
  /**
   * Instance 的编号的缓存
   *
   * KEY：{@link #assembleInstanceKey(String, String, String, String)}
   * VALUE：{@link Instance#id}
   */
  private Cache<String, Long> instanceCache;
  /**
   * InstanceConfig 的 ReleaseKey 的缓存
   *
   * KEY：{@link #assembleInstanceConfigKey(long, String, String)}
   * VALUE：{@link InstanceConfig#id}
   */
  private Cache<String, String> instanceConfigReleaseKeyCache;

  private final InstanceService instanceService;

  public InstanceConfigAuditUtil(final InstanceService instanceService) {
    this.instanceService = instanceService;
    auditExecutorService = Executors.newSingleThreadExecutor(
        ApolloThreadFactory.create("InstanceConfigAuditUtil", true));
    auditStopped = new AtomicBoolean(false);
    instanceCache = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.HOURS)
        .maximumSize(INSTANCE_CACHE_MAX_SIZE).build();
    instanceConfigReleaseKeyCache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.DAYS)
        .maximumSize(INSTANCE_CONFIG_CACHE_MAX_SIZE).build();
  }

  public boolean audit(String appId, String clusterName, String dataCenter, String
      ip, String configAppId, String configClusterName, String configNamespace, String releaseKey) {
    return this.audits.offer(new InstanceConfigAuditModel(appId, clusterName, dataCenter, ip,
        configAppId, configClusterName, configNamespace, releaseKey));
  }

  void doAudit(InstanceConfigAuditModel auditModel) {
    // 拼接 instanceCache 的 KEY
    String instanceCacheKey = assembleInstanceKey(auditModel.getAppId(), auditModel
        .getClusterName(), auditModel.getIp(), auditModel.getDataCenter());
    // 获得 Instance 编号
    Long instanceId = instanceCache.getIfPresent(instanceCacheKey);
    // 查询不到，从 DB 加载或者创建，并添加到缓存中。
    if (instanceId == null) {
      instanceId = prepareInstanceId(auditModel);
      instanceCache.put(instanceCacheKey, instanceId);
    }
    // 获得 instanceConfigReleaseKeyCache 的 KEY
    //load instance config release key from cache, and check if release key is the same
    String instanceConfigCacheKey = assembleInstanceConfigKey(instanceId, auditModel
        .getConfigAppId(), auditModel.getConfigNamespace());
    // 获得缓存的 cacheReleaseKey
    String cacheReleaseKey = instanceConfigReleaseKeyCache.getIfPresent(instanceConfigCacheKey);

    //if release key is the same, then skip audit // 若相等，跳过
    if (cacheReleaseKey != null && Objects.equals(cacheReleaseKey, auditModel.getReleaseKey())) {
      return;
    }
    // 更新对应的 instanceConfigReleaseKeyCache 缓存
    instanceConfigReleaseKeyCache.put(instanceConfigCacheKey, auditModel.getReleaseKey());
    // 获得 InstanceConfig 对象
    //if release key is not the same or cannot find in cache, then do audit
    InstanceConfig instanceConfig = instanceService.findInstanceConfig(instanceId, auditModel
        .getConfigAppId(), auditModel.getConfigNamespace());
    // 若 InstanceConfig 已经存在，进行更新
    if (instanceConfig != null) {
      // ReleaseKey 发生变化
      if (!Objects.equals(instanceConfig.getReleaseKey(), auditModel.getReleaseKey())) {
        instanceConfig.setConfigClusterName(auditModel.getConfigClusterName());
        instanceConfig.setReleaseKey(auditModel.getReleaseKey());
        instanceConfig.setReleaseDeliveryTime(auditModel.getOfferTime());
        // 时间过近，例如 Client 先请求的 Config Service A 节点，再请求 Config Service B 节点的情况。
      } else if (offerTimeAndLastModifiedTimeCloseEnough(auditModel.getOfferTime(),
          instanceConfig.getDataChangeLastModifiedTime())) {
        //when releaseKey is the same, optimize to reduce writes if the record was updated not long ago
        return;
      }
      // 更新
      //we need to update no matter the release key is the same or not, to ensure the
      //last modified time is updated each day
      instanceConfig.setDataChangeLastModifiedTime(auditModel.getOfferTime());
      instanceService.updateInstanceConfig(instanceConfig);
      return;
    }
    // 若 InstanceConfig 不存在，创建 InstanceConfig 对象
    instanceConfig = new InstanceConfig();
    instanceConfig.setInstanceId(instanceId);
    instanceConfig.setConfigAppId(auditModel.getConfigAppId());
    instanceConfig.setConfigClusterName(auditModel.getConfigClusterName());
    instanceConfig.setConfigNamespaceName(auditModel.getConfigNamespace());
    instanceConfig.setReleaseKey(auditModel.getReleaseKey());
    instanceConfig.setReleaseDeliveryTime(auditModel.getOfferTime());
    instanceConfig.setDataChangeCreatedTime(auditModel.getOfferTime());

    try {
      // 保存 InstanceConfig 对象到数据库中
      instanceService.createInstanceConfig(instanceConfig);
    } catch (DataIntegrityViolationException ex) {
      //concurrent insertion, safe to ignore
    }
  }

  private boolean offerTimeAndLastModifiedTimeCloseEnough(Date offerTime, Date lastModifiedTime) {
    return (offerTime.getTime() - lastModifiedTime.getTime()) <
        OFFER_TIME_LAST_MODIFIED_TIME_THRESHOLD_IN_MILLI;
  }

  private long prepareInstanceId(InstanceConfigAuditModel auditModel) {
    // 查询 Instance 对象
    Instance instance = instanceService.findInstance(auditModel.getAppId(), auditModel
        .getClusterName(), auditModel.getDataCenter(), auditModel.getIp());
    // 已存在，返回 Instance 编号
    if (instance != null) {
      return instance.getId();
    }
    // 若 Instance 不存在，创建 Instance 对象
    instance = new Instance();
    instance.setAppId(auditModel.getAppId());
    instance.setClusterName(auditModel.getClusterName());
    instance.setDataCenter(auditModel.getDataCenter());
    instance.setIp(auditModel.getIp());


    try {
      // 保存 Instance 对象到数据库中
      return instanceService.createInstance(instance).getId();
    } catch (DataIntegrityViolationException ex) {
      //return the one exists // 发生唯一索引冲突，意味着已经存在，进行查询 Instance 对象，并返回
      return instanceService.findInstance(instance.getAppId(), instance.getClusterName(),
          instance.getDataCenter(), instance.getIp()).getId();
    }
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    // 提交任务
    auditExecutorService.submit(() -> {
      // 循环，直到停止或线程打断
      while (!auditStopped.get() && !Thread.currentThread().isInterrupted()) {
        try {
          // 获得队首 InstanceConfigAuditModel 元素，非阻塞
          InstanceConfigAuditModel model = audits.poll();
          // 若获取不到，sleep 等待 1 秒
          if (model == null) {
            TimeUnit.SECONDS.sleep(1);
            continue;
          }
          // 若获取到，记录 Instance 和 InstanceConfig
          doAudit(model);
        } catch (Throwable ex) {
          Tracer.logError(ex);
        }
      }
    });
  }

  private String assembleInstanceKey(String appId, String cluster, String ip, String datacenter) {
    List<String> keyParts = Lists.newArrayList(appId, cluster, ip);
    if (!Strings.isNullOrEmpty(datacenter)) {
      keyParts.add(datacenter);
    }
    return STRING_JOINER.join(keyParts);
  }

  private String assembleInstanceConfigKey(long instanceId, String configAppId, String configNamespace) {
    return STRING_JOINER.join(instanceId, configAppId, configNamespace);
  }

  public static class InstanceConfigAuditModel {
    private String appId;
    private String clusterName;
    private String dataCenter;
    private String ip;
    private String configAppId;
    private String configClusterName;
    private String configNamespace;
    private String releaseKey;
    /**
     * 入队时间
     */
    private Date offerTime;

    public InstanceConfigAuditModel(String appId, String clusterName, String dataCenter, String
        clientIp, String configAppId, String configClusterName, String configNamespace, String
                                        releaseKey) {
      this.offerTime = new Date();
      this.appId = appId;
      this.clusterName = clusterName;
      this.dataCenter = Strings.isNullOrEmpty(dataCenter) ? "" : dataCenter;
      this.ip = clientIp;
      this.configAppId = configAppId;
      this.configClusterName = configClusterName;
      this.configNamespace = configNamespace;
      this.releaseKey = releaseKey;
    }

    public String getAppId() {
      return appId;
    }

    public String getClusterName() {
      return clusterName;
    }

    public String getDataCenter() {
      return dataCenter;
    }

    public String getIp() {
      return ip;
    }

    public String getConfigAppId() {
      return configAppId;
    }

    public String getConfigNamespace() {
      return configNamespace;
    }

    public String getReleaseKey() {
      return releaseKey;
    }

    public String getConfigClusterName() {
      return configClusterName;
    }

    public Date getOfferTime() {
      return offerTime;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      InstanceConfigAuditModel model = (InstanceConfigAuditModel) o;
      return Objects.equals(appId, model.appId) &&
          Objects.equals(clusterName, model.clusterName) &&
          Objects.equals(dataCenter, model.dataCenter) &&
          Objects.equals(ip, model.ip) &&
          Objects.equals(configAppId, model.configAppId) &&
          Objects.equals(configClusterName, model.configClusterName) &&
          Objects.equals(configNamespace, model.configNamespace) &&
          Objects.equals(releaseKey, model.releaseKey);
    }

    @Override
    public int hashCode() {
      return Objects.hash(appId, clusterName, dataCenter, ip, configAppId, configClusterName,
          configNamespace,
          releaseKey);
    }
  }
}

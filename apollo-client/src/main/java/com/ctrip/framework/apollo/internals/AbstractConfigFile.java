package com.ctrip.framework.apollo.internals;

import com.ctrip.framework.apollo.enums.ConfigSourceType;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.framework.apollo.ConfigFile;
import com.ctrip.framework.apollo.ConfigFileChangeListener;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.enums.PropertyChangeType;
import com.ctrip.framework.apollo.model.ConfigFileChangeEvent;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.ctrip.framework.apollo.util.ExceptionUtil;
import com.google.common.collect.Lists;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public abstract class AbstractConfigFile implements ConfigFile, RepositoryChangeListener {
  private static final Logger logger = LoggerFactory.getLogger(AbstractConfigFile.class);
  private static ExecutorService m_executorService;
  protected final ConfigRepository m_configRepository;
  protected final String m_namespace;
  protected final AtomicReference<Properties> m_configProperties;
  private final List<ConfigFileChangeListener> m_listeners = Lists.newCopyOnWriteArrayList();

  private volatile ConfigSourceType m_sourceType = ConfigSourceType.NONE;

  static {
    m_executorService = Executors.newCachedThreadPool(ApolloThreadFactory
        .create("ConfigFile", true));
  }

  public AbstractConfigFile(String namespace, ConfigRepository configRepository) {
    m_configRepository = configRepository;
    m_namespace = namespace;
    m_configProperties = new AtomicReference<>();
    initialize();
  }

  private void initialize() {
    try {
      m_configProperties.set(m_configRepository.getConfig());
      m_sourceType = m_configRepository.getSourceType();
    } catch (Throwable ex) {
      Tracer.logError(ex);
      logger.warn("Init Apollo Config File failed - namespace: {}, reason: {}.",
          m_namespace, ExceptionUtil.getDetailMessage(ex));
    } finally {
      //register the change listener no matter config repository is working or not
      //so that whenever config repository is recovered, config could get changed
      m_configRepository.addChangeListener(this);
    }
  }

  @Override
  public String getNamespace() {
    return m_namespace;
  }

  protected abstract void update(Properties newProperties);

  @Override
  public synchronized void onRepositoryChange(String namespace, Properties newProperties) {
    // 新配置与旧配置比较
    if (newProperties.equals(m_configProperties.get())) {
      return;
    }
    Properties newConfigProperties = new Properties();
    newConfigProperties.putAll(newProperties);
    // 获取旧值
    String oldValue = getContent();
    // 更新新值（新值、缓存）
    update(newProperties);
    m_sourceType = m_configRepository.getSourceType();
    // 获取更新后的值
    String newValue = getContent();
    // 修改类型
    PropertyChangeType changeType = PropertyChangeType.MODIFIED;

    if (oldValue == null) {// 旧值为空则为 新增操作
      changeType = PropertyChangeType.ADDED;
    } else if (newValue == null) {// 新值为空则为 删除操作
      changeType = PropertyChangeType.DELETED;
    }
    // 通知事件 （新值 、旧值、类型）
    this.fireConfigChange(new ConfigFileChangeEvent(m_namespace, oldValue, newValue, changeType));

    Tracer.logEvent("Apollo.Client.ConfigChanges", m_namespace);
  }

  @Override
  public void addChangeListener(ConfigFileChangeListener listener) {
    if (!m_listeners.contains(listener)) {
      m_listeners.add(listener);
    }
  }

  @Override
  public boolean removeChangeListener(ConfigFileChangeListener listener) {
    return m_listeners.remove(listener);
  }

  @Override
  public ConfigSourceType getSourceType() {
    return m_sourceType;
  }

  private void fireConfigChange(final ConfigFileChangeEvent changeEvent) {
    // 遍历所有的监听器
    for (final ConfigFileChangeListener listener : m_listeners) {
      m_executorService.submit(new Runnable() {
        @Override
        public void run() {
          String listenerName = listener.getClass().getName();
          Transaction transaction = Tracer.newTransaction("Apollo.ConfigFileChangeListener", listenerName);
          try {
            // 通知时间开始
            listener.onChange(changeEvent);
            transaction.setStatus(Transaction.SUCCESS);
          } catch (Throwable ex) {
            transaction.setStatus(ex);
            Tracer.logError(ex);
            logger.error("Failed to invoke config file change listener {}", listenerName, ex);
          } finally {
            transaction.complete();
          }
        }
      });
    }
  }
}

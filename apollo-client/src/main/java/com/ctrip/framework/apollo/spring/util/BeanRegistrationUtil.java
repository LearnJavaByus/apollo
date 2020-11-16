package com.ctrip.framework.apollo.spring.util;

import java.util.Map;
import java.util.Objects;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class BeanRegistrationUtil {
  public static boolean registerBeanDefinitionIfNotExists(BeanDefinitionRegistry registry, String beanName,
      Class<?> beanClass) {
    return registerBeanDefinitionIfNotExists(registry, beanName, beanClass, null);
  }

  /**
   * 注册 beanClass 到 BeanDefinitionRegistry 中，当且仅当 beanName 和 beanClass 都不存在对应的 BeanDefinition 时。
   * @param registry
   * @param beanName
   * @param beanClass
   * @param extraPropertyValues
   * @return
   */
  public static boolean registerBeanDefinitionIfNotExists(BeanDefinitionRegistry registry, String beanName,
                                                          Class<?> beanClass, Map<String, Object> extraPropertyValues) {
    // 不存在 `beanName` 对应的 BeanDefinition
    if (registry.containsBeanDefinition(beanName)) {
      return false;
    }
    // 不存在 `beanClass` 对应的 BeanDefinition
    String[] candidates = registry.getBeanDefinitionNames();

    for (String candidate : candidates) {
      BeanDefinition beanDefinition = registry.getBeanDefinition(candidate);
      if (Objects.equals(beanDefinition.getBeanClassName(), beanClass.getName())) {
        return false;
      }
    }
    // 注册 `beanClass` 到 BeanDefinitionRegistry 中
    BeanDefinition beanDefinition = BeanDefinitionBuilder.genericBeanDefinition(beanClass).getBeanDefinition();

    if (extraPropertyValues != null) {
      for (Map.Entry<String, Object> entry : extraPropertyValues.entrySet()) {
        beanDefinition.getPropertyValues().add(entry.getKey(), entry.getValue());
      }
    }

    registry.registerBeanDefinition(beanName, beanDefinition);

    return true;
  }


}

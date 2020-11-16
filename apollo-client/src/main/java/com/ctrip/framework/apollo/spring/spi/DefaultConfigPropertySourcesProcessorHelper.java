package com.ctrip.framework.apollo.spring.spi;

import com.ctrip.framework.apollo.core.spi.Ordered;
import com.ctrip.framework.apollo.spring.annotation.ApolloAnnotationProcessor;
import com.ctrip.framework.apollo.spring.annotation.ApolloJsonValueProcessor;
import com.ctrip.framework.apollo.spring.annotation.SpringValueProcessor;
import com.ctrip.framework.apollo.spring.property.SpringValueDefinitionProcessor;
import com.ctrip.framework.apollo.spring.util.BeanRegistrationUtil;
import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

public class DefaultConfigPropertySourcesProcessorHelper implements ConfigPropertySourcesProcessorHelper {

  @Override
  public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
    Map<String, Object> propertySourcesPlaceholderPropertyValues = new HashMap<>();
    // to make sure the default PropertySourcesPlaceholderConfigurer's priority is higher than PropertyPlaceholderConfigurer
    propertySourcesPlaceholderPropertyValues.put("order", 0);
    // 注册 PropertySourcesPlaceholderConfigurer 到 BeanDefinitionRegistry 中，替换 PlaceHolder 为对应的属性值，
    // 参考文章 https://leokongwq.github.io/2016/12/28/spring-PropertyPlaceholderConfigurer.html
    BeanRegistrationUtil.registerBeanDefinitionIfNotExists(registry, PropertySourcesPlaceholderConfigurer.class.getName(),
        PropertySourcesPlaceholderConfigurer.class, propertySourcesPlaceholderPropertyValues);
    // 注册 ApolloAnnotationProcessor 到 BeanDefinitionRegistry 中，因为 XML 配置的 Bean 对象，也可能存在 @ApolloConfig 和 @ApolloConfigChangeListener 注解。
    BeanRegistrationUtil.registerBeanDefinitionIfNotExists(registry, ApolloAnnotationProcessor.class.getName(),
        ApolloAnnotationProcessor.class);
    // 注册 SpringValueProcessor 到 BeanDefinitionRegistry 中，用于 PlaceHolder 自动更新机制
    BeanRegistrationUtil.registerBeanDefinitionIfNotExists(registry, SpringValueProcessor.class.getName(),
        SpringValueProcessor.class);
    // 注册 ApolloJsonValueProcessor 到 BeanDefinitionRegistry 中，因为 XML 配置的 Bean 对象，也可能存在 @ApolloJsonValue 注解。
    BeanRegistrationUtil.registerBeanDefinitionIfNotExists(registry, ApolloJsonValueProcessor.class.getName(),
        ApolloJsonValueProcessor.class);
    // 处理 XML 配置的 Spring PlaceHolder
    processSpringValueDefinition(registry);
  }

  /**
   * For Spring 3.x versions, the BeanDefinitionRegistryPostProcessor would not be instantiated if
   * it is added in postProcessBeanDefinitionRegistry phase, so we have to manually call the
   * postProcessBeanDefinitionRegistry method of SpringValueDefinitionProcessor here...
   */
  private void processSpringValueDefinition(BeanDefinitionRegistry registry) {
    // 创建 SpringValueDefinitionProcessor 对象
    SpringValueDefinitionProcessor springValueDefinitionProcessor = new SpringValueDefinitionProcessor();
    // 处理 XML 配置的 Spring PlaceHolder
    springValueDefinitionProcessor.postProcessBeanDefinitionRegistry(registry);
  }

  @Override
  public int getOrder() {
    return Ordered.LOWEST_PRECEDENCE;
  }
}

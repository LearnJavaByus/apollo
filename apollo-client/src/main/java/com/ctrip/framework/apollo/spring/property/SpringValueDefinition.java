package com.ctrip.framework.apollo.spring.property;

public class SpringValueDefinition {
  /**
   * KEY
   *
   * 即在 Config 中的属性 KEY 。
   */
  private final String key;
  /**
   * 占位符
   */
  private final String placeholder;
  /**
   * 属性名
   */
  private final String propertyName;

  public SpringValueDefinition(String key, String placeholder, String propertyName) {
    this.key = key;
    this.placeholder = placeholder;
    this.propertyName = propertyName;
  }

  public String getKey() {
    return key;
  }

  public String getPlaceholder() {
    return placeholder;
  }

  public String getPropertyName() {
    return propertyName;
  }
}

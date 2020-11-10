package com.ctrip.framework.apollo.biz.repository;

import com.ctrip.framework.apollo.biz.entity.NamespaceLock;

import org.springframework.data.repository.PagingAndSortingRepository;

/**
 * 提供 NamespaceLock 的数据访问 给 Admin Service 和 Config Service
 */
public interface NamespaceLockRepository extends PagingAndSortingRepository<NamespaceLock, Long> {

  NamespaceLock findByNamespaceId(Long namespaceId);

  Long deleteByNamespaceId(Long namespaceId);

}

package com.ctrip.framework.apollo.biz.service;

import com.ctrip.framework.apollo.biz.entity.Audit;
import com.ctrip.framework.apollo.biz.entity.Cluster;
import com.ctrip.framework.apollo.biz.entity.GrayReleaseRule;
import com.ctrip.framework.apollo.biz.entity.Namespace;
import com.ctrip.framework.apollo.biz.entity.Release;
import com.ctrip.framework.apollo.biz.repository.GrayReleaseRuleRepository;
import com.ctrip.framework.apollo.common.constants.NamespaceBranchStatus;
import com.ctrip.framework.apollo.common.constants.ReleaseOperation;
import com.ctrip.framework.apollo.common.constants.ReleaseOperationContext;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.common.utils.GrayReleaseRuleItemTransformer;
import com.ctrip.framework.apollo.common.utils.UniqueKeyGenerator;
import com.google.common.collect.Maps;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

@Service
public class NamespaceBranchService {

  private final AuditService auditService;
  private final GrayReleaseRuleRepository grayReleaseRuleRepository;
  private final ClusterService clusterService;
  private final ReleaseService releaseService;
  private final NamespaceService namespaceService;
  private final ReleaseHistoryService releaseHistoryService;

  public NamespaceBranchService(
      final AuditService auditService,
      final GrayReleaseRuleRepository grayReleaseRuleRepository,
      final ClusterService clusterService,
      final @Lazy ReleaseService releaseService,
      final NamespaceService namespaceService,
      final ReleaseHistoryService releaseHistoryService) {
    this.auditService = auditService;
    this.grayReleaseRuleRepository = grayReleaseRuleRepository;
    this.clusterService = clusterService;
    this.releaseService = releaseService;
    this.namespaceService = namespaceService;
    this.releaseHistoryService = releaseHistoryService;
  }

  @Transactional
  public Namespace createBranch(String appId, String parentClusterName, String namespaceName, String operator){
    // 获得子 Namespace 对象
    Namespace childNamespace = findBranch(appId, parentClusterName, namespaceName);
    // 若存在子 Namespace 对象，则抛出 BadRequestException 异常。一个 Namespace 有且仅允许有一个子 Namespace 。
    if (childNamespace != null){
      throw new BadRequestException("namespace already has branch");
    }
    // 获得父 Cluster 对象
    Cluster parentCluster = clusterService.findOne(appId, parentClusterName);
    // 若父 Cluster 对象不存在，抛出 BadRequestException 异常
    if (parentCluster == null || parentCluster.getParentClusterId() != 0) {
      throw new BadRequestException("cluster not exist or illegal cluster");
    }

    //create child cluster // 创建子 Cluster 对象
    Cluster childCluster = createChildCluster(appId, parentCluster, namespaceName, operator);
    // 保存子 Cluster 对象
    Cluster createdChildCluster = clusterService.saveWithoutInstanceOfAppNamespaces(childCluster);

    //create child namespace // 创建子 Namespace 对象
    childNamespace = createNamespaceBranch(appId, createdChildCluster.getName(),
                                                        namespaceName, operator);
    // 保存子 Namespace 对象
    return namespaceService.save(childNamespace);
  }

  public Namespace findBranch(String appId, String parentClusterName, String namespaceName) {
    return namespaceService.findChildNamespace(appId, parentClusterName, namespaceName);
  }

  public GrayReleaseRule findBranchGrayRules(String appId, String clusterName, String namespaceName,
                                             String branchName) {
    return grayReleaseRuleRepository
        .findTopByAppIdAndClusterNameAndNamespaceNameAndBranchNameOrderByIdDesc(appId, clusterName, namespaceName, branchName);
  }

  @Transactional
  public void updateBranchGrayRules(String appId, String clusterName, String namespaceName,
                                    String branchName, GrayReleaseRule newRules) {
    doUpdateBranchGrayRules(appId, clusterName, namespaceName, branchName, newRules, true, ReleaseOperation.APPLY_GRAY_RULES);
  }

  private void doUpdateBranchGrayRules(String appId, String clusterName, String namespaceName,
                                              String branchName, GrayReleaseRule newRules, boolean recordReleaseHistory, int releaseOperation) {
    GrayReleaseRule oldRules = grayReleaseRuleRepository
        .findTopByAppIdAndClusterNameAndNamespaceNameAndBranchNameOrderByIdDesc(appId, clusterName, namespaceName, branchName);

    Release latestBranchRelease = releaseService.findLatestActiveRelease(appId, branchName, namespaceName);

    long latestBranchReleaseId = latestBranchRelease != null ? latestBranchRelease.getId() : 0;

    newRules.setReleaseId(latestBranchReleaseId);

    grayReleaseRuleRepository.save(newRules);

    //delete old rules
    if (oldRules != null) {
      grayReleaseRuleRepository.delete(oldRules);
    }

    if (recordReleaseHistory) {
      Map<String, Object> releaseOperationContext = Maps.newHashMap();
      releaseOperationContext.put(ReleaseOperationContext.RULES, GrayReleaseRuleItemTransformer
          .batchTransformFromJSON(newRules.getRules()));
      if (oldRules != null) {
        releaseOperationContext.put(ReleaseOperationContext.OLD_RULES,
            GrayReleaseRuleItemTransformer.batchTransformFromJSON(oldRules.getRules()));
      }
      releaseHistoryService.createReleaseHistory(appId, clusterName, namespaceName, branchName, latestBranchReleaseId,
          latestBranchReleaseId, releaseOperation, releaseOperationContext, newRules.getDataChangeLastModifiedBy());
    }
  }

  @Transactional
  public GrayReleaseRule updateRulesReleaseId(String appId, String clusterName,
                                   String namespaceName, String branchName,
                                   long latestReleaseId, String operator) {
    GrayReleaseRule oldRules = grayReleaseRuleRepository.
        findTopByAppIdAndClusterNameAndNamespaceNameAndBranchNameOrderByIdDesc(appId, clusterName, namespaceName, branchName);

    if (oldRules == null) {
      return null;
    }

    GrayReleaseRule newRules = new GrayReleaseRule();
    newRules.setBranchStatus(NamespaceBranchStatus.ACTIVE);
    newRules.setReleaseId(latestReleaseId);
    newRules.setRules(oldRules.getRules());
    newRules.setAppId(oldRules.getAppId());
    newRules.setClusterName(oldRules.getClusterName());
    newRules.setNamespaceName(oldRules.getNamespaceName());
    newRules.setBranchName(oldRules.getBranchName());
    newRules.setDataChangeCreatedBy(operator);
    newRules.setDataChangeLastModifiedBy(operator);

    grayReleaseRuleRepository.save(newRules);

    grayReleaseRuleRepository.delete(oldRules);

    return newRules;
  }

  @Transactional
  public void deleteBranch(String appId, String clusterName, String namespaceName,
                           String branchName, int branchStatus, String operator) {
    Cluster toDeleteCluster = clusterService.findOne(appId, branchName);
    if (toDeleteCluster == null) {
      return;
    }

    Release latestBranchRelease = releaseService.findLatestActiveRelease(appId, branchName, namespaceName);

    long latestBranchReleaseId = latestBranchRelease != null ? latestBranchRelease.getId() : 0;

    //update branch rules
    GrayReleaseRule deleteRule = new GrayReleaseRule();
    deleteRule.setRules("[]");
    deleteRule.setAppId(appId);
    deleteRule.setClusterName(clusterName);
    deleteRule.setNamespaceName(namespaceName);
    deleteRule.setBranchName(branchName);
    deleteRule.setBranchStatus(branchStatus);
    deleteRule.setDataChangeLastModifiedBy(operator);
    deleteRule.setDataChangeCreatedBy(operator);

    doUpdateBranchGrayRules(appId, clusterName, namespaceName, branchName, deleteRule, false, -1);

    //delete branch cluster
    clusterService.delete(toDeleteCluster.getId(), operator);

    int releaseOperation = branchStatus == NamespaceBranchStatus.MERGED ? ReleaseOperation
        .GRAY_RELEASE_DELETED_AFTER_MERGE : ReleaseOperation.ABANDON_GRAY_RELEASE;

    releaseHistoryService.createReleaseHistory(appId, clusterName, namespaceName, branchName, latestBranchReleaseId,
        latestBranchReleaseId, releaseOperation, null, operator);

    auditService.audit("Branch", toDeleteCluster.getId(), Audit.OP.DELETE, operator);
  }

  private Cluster createChildCluster(String appId, Cluster parentCluster,
                                     String namespaceName, String operator) {

    Cluster childCluster = new Cluster();
    childCluster.setAppId(appId);
    childCluster.setParentClusterId(parentCluster.getId());
    childCluster.setName(UniqueKeyGenerator.generate(appId, parentCluster.getName(), namespaceName));
    childCluster.setDataChangeCreatedBy(operator);
    childCluster.setDataChangeLastModifiedBy(operator);

    return childCluster;
  }


  private Namespace createNamespaceBranch(String appId, String clusterName, String namespaceName, String operator) {
    Namespace childNamespace = new Namespace();
    childNamespace.setAppId(appId);
    childNamespace.setClusterName(clusterName);
    childNamespace.setNamespaceName(namespaceName);
    childNamespace.setDataChangeLastModifiedBy(operator);
    childNamespace.setDataChangeCreatedBy(operator);
    return childNamespace;
  }

}

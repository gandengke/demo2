package com.jd.easy.audience.task.commonbean.bean;

/**
 * rfm算法调用的配置信息
 *
 * @author cdxiongmei
 * @version 1.0
 */
public class RfmAnalysisConfig {
    private String jfsBucket;
    private String jfsEndPoint;
    private String accessKey;
    private String secretKey;
    private String esNode;
    private String esPort;
    private String esUser;
    private String esPassword;
    //ES表名：购买人群规模+购买行为概览
    private String purchaseBehaviorInfo;
    //ES表名：最近购买时长分布
    private String purchaseDurationDistribution;
    //ES表名：购买频次分布
    private String purchaseFrequencyDistribution;
    //ES表名：复购周期分布(平均购买间隔分布)
    private String repurchaseCycleDistribution;
    //ES表名：消费金额分布
    private String monetaryDistribution;
    //ES表名：购买数量分布
    private String purchaseAmountDistribution;
    //ES表名：购买渠道分布
    private String purchaseChannelDistribution;
    //ES表名：用户分层分析
    private String consumerLayer;
    public String getJfsBucket() {
        return jfsBucket;
    }

    public void setJfsBucket(String jfsBucket) {
        this.jfsBucket = jfsBucket;
    }

    public String getJfsEndPoint() {
        return jfsEndPoint;
    }

    public void setJfsEndPoint(String jfsEndPoint) {
        this.jfsEndPoint = jfsEndPoint;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getPurchaseBehaviorInfo() {
        return purchaseBehaviorInfo;
    }

    public void setPurchaseBehaviorInfo(String purchaseBehaviorInfo) {
        this.purchaseBehaviorInfo = purchaseBehaviorInfo;
    }

    public String getPurchaseDurationDistribution() {
        return purchaseDurationDistribution;
    }

    public void setPurchaseDurationDistribution(String purchaseDurationDistribution) {
        this.purchaseDurationDistribution = purchaseDurationDistribution;
    }

    public String getPurchaseFrequencyDistribution() {
        return purchaseFrequencyDistribution;
    }

    public void setPurchaseFrequencyDistribution(String purchaseFrequencyDistribution) {
        this.purchaseFrequencyDistribution = purchaseFrequencyDistribution;
    }

    public String getRepurchaseCycleDistribution() {
        return repurchaseCycleDistribution;
    }

    public void setRepurchaseCycleDistribution(String repurchaseCycleDistribution) {
        this.repurchaseCycleDistribution = repurchaseCycleDistribution;
    }

    public String getMonetaryDistribution() {
        return monetaryDistribution;
    }

    public void setMonetaryDistribution(String monetaryDistribution) {
        this.monetaryDistribution = monetaryDistribution;
    }

    public String getPurchaseAmountDistribution() {
        return purchaseAmountDistribution;
    }

    public void setPurchaseAmountDistribution(String purchaseAmountDistribution) {
        this.purchaseAmountDistribution = purchaseAmountDistribution;
    }

    public String getPurchaseChannelDistribution() {
        return purchaseChannelDistribution;
    }

    public void setPurchaseChannelDistribution(String purchaseChannelDistribution) {
        this.purchaseChannelDistribution = purchaseChannelDistribution;
    }

    public String getConsumerLayer() {
        return consumerLayer;
    }

    public void setConsumerLayer(String consumerLayer) {
        this.consumerLayer = consumerLayer;
    }

    public String getEsNode() {
        return esNode;
    }

    public void setEsNode(String esNode) {
        this.esNode = esNode;
    }

    public String getEsPort() {
        return esPort;
    }

    public void setEsPort(String esPort) {
        this.esPort = esPort;
    }

    public String getEsUser() {
        return esUser;
    }

    public void setEsUser(String esUser) {
        this.esUser = esUser;
    }

    public String getEsPassword() {
        return esPassword;
    }

    public void setEsPassword(String esPassword) {
        this.esPassword = esPassword;
    }
}

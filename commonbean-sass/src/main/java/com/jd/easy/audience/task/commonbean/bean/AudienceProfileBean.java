package com.jd.easy.audience.task.commonbean.bean;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.jd.easy.audience.task.driven.step.StepCommonBean;

import java.util.List;
import java.util.Map;

/**
 * 人群透视分析的定义
 *
 * @author cdxiongmei
 * @version 1.0
 */
@Deprecated
@JsonTypeName(value = "audienceProfileBean")
public class AudienceProfileBean extends StepCommonBean<Void> {
    private String endpoint;
    private String bucket;
    private String accessKey;
    private String secretKey;
    private String esNode;
    private String esPort;
    private String esUser;
    private String esPass;
    private String esIndex;
    private String audienceDataFile;
    private Long audienceId;

    //标签数据集对应的标签字段
    private Map<Long, List<JSONObject>> labelDetail;
    private Map<Long, String> labelName;
    //标签数据集的文件路径(key为标签数据集的id)-value为osspath/filename
    private Map<Long, String> labelFilePath;

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
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

    public String getEsPass() {
        return esPass;
    }

    public void setEsPass(String esPass) {
        this.esPass = esPass;
    }

    public String getEsIndex() {
        return esIndex;
    }

    public void setEsIndex(String esIndex) {
        this.esIndex = esIndex;
    }

    public String getAudienceDataFile() {
        return audienceDataFile;
    }

    public void setAudienceDataFile(String audienceDataFile) {
        this.audienceDataFile = audienceDataFile;
    }

    public Long getAudienceId() {
        return audienceId;
    }

    public void setAudienceId(Long audienceId) {
        this.audienceId = audienceId;
    }

    public Map<Long, List<JSONObject>> getLabelDetail() {
        return labelDetail;
    }

    public void setLabelDetail(Map<Long, List<JSONObject>> labelDetail) {
        this.labelDetail = labelDetail;
    }

    public Map<Long, String> getLabelName() {
        return labelName;
    }

    public void setLabelName(Map<Long, String> labelName) {
        this.labelName = labelName;
    }

    public Map<Long, String> getLabelFilePath() {
        return labelFilePath;
    }

    public void setLabelFilePath(Map<Long, String> labelFilePath) {
        this.labelFilePath = labelFilePath;
    }

    @Override
    public void setStepClassName(String stepClassName) {
        super.setStepClassName("com.jd.easy.audience.task.plugin.step.audience.AudienceLabelDatasetProfileStep");
    }

}

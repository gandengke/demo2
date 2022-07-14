package com.jd.easy.audience.task.commonbean.bean;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.jd.easy.audience.task.driven.step.StepCommonBean;

import java.util.Map;

/**
 * 人群上传任务参数的定义
 *
 * @author cdxiongmei
 * @version 1.0
 */
@JsonTypeName(value = "audienceJfsUploadBean")
public class AudienceJfsUploadBean extends StepCommonBean<Map<String, Object>> {
    private String audienceDatasetStepName;
    // 文件路径不包含文件名
    private String jfsFilePath;
    private String fileName;

    private String jfsBucket;
    private String jfsAccessKey;
    private String jfsSecretKey;
    private String jfsEndPoint;
    //人群包最大值
    private String audienceLimitNum;

    // 输出属性
    private String audiencePackageUrl;
    private String audienceJfsObjectKey;
    private String audienceJfsFileMd5;

    private String audienceType;

    private Boolean ifLimit;

    public String getAudienceDatasetStepName() {
        return audienceDatasetStepName;
    }

    public void setAudienceDatasetStepName(String audienceDatasetStepName) {
        this.audienceDatasetStepName = audienceDatasetStepName;
    }

    public String getJfsFilePath() {
        return jfsFilePath;
    }

    public void setJfsFilePath(String jfsFilePath) {
        this.jfsFilePath = jfsFilePath;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getJfsBucket() {
        return jfsBucket;
    }

    public void setJfsBucket(String jfsBucket) {
        this.jfsBucket = jfsBucket;
    }

    public String getJfsAccessKey() {
        return jfsAccessKey;
    }

    public void setJfsAccessKey(String jfsAccessKey) {
        this.jfsAccessKey = jfsAccessKey;
    }

    public String getJfsSecretKey() {
        return jfsSecretKey;
    }

    public void setJfsSecretKey(String jfsSecretKey) {
        this.jfsSecretKey = jfsSecretKey;
    }

    public String getJfsEndPoint() {
        return jfsEndPoint;
    }

    public void setJfsEndPoint(String jfsEndPoint) {
        this.jfsEndPoint = jfsEndPoint;
    }

    public String getAudienceLimitNum() {
        return audienceLimitNum;
    }

    public void setAudienceLimitNum(String audienceLimitNum) {
        this.audienceLimitNum = audienceLimitNum;
    }

    public String getAudiencePackageUrl() {
        return audiencePackageUrl;
    }

    public void setAudiencePackageUrl(String audiencePackageUrl) {
        this.audiencePackageUrl = audiencePackageUrl;
    }

    public String getAudienceJfsObjectKey() {
        return audienceJfsObjectKey;
    }

    public void setAudienceJfsObjectKey(String audienceJfsObjectKey) {
        this.audienceJfsObjectKey = audienceJfsObjectKey;
    }

    public String getAudienceJfsFileMd5() {
        return audienceJfsFileMd5;
    }

    public void setAudienceJfsFileMd5(String audienceJfsFileMd5) {
        this.audienceJfsFileMd5 = audienceJfsFileMd5;
    }

    public String getAudienceType() {
        return audienceType;
    }

    public void setAudienceType(String audienceType) {
        this.audienceType = audienceType;
    }

    public Boolean getIfLimit() {
        return ifLimit;
    }

    public void setIfLimit(Boolean ifLimit) {
        this.ifLimit = ifLimit;
    }

    @Override
    public void setStepClassName(String stepClassName) {
        super.setStepClassName("com.jd.easy.audience.task.plugin.step.audience.AudienceJfsUploadStep");
    }

}

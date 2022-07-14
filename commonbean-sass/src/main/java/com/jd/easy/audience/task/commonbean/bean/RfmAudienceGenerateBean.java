package com.jd.easy.audience.task.commonbean.bean;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.jd.easy.audience.common.audience.AudienceSourceTypeEnum;
import com.jd.easy.audience.task.driven.step.StepCommonBean;

import java.util.Map;

/**
 * rfm人群生成参数的定义
 *
 * @author cdxiongmei
 * @version 1.0
 */
@JsonTypeName(value = "rfmAudienceGenerateBean")
public class RfmAudienceGenerateBean extends StepCommonBean<Map<String, Object>> {
    private String audienceId;
    // 输出人群包
    private String jfsFilePath;
    private String fileName;

    //parquet文件路径：osspath/filename
    private String inputFilePath;

    private String jfsBucket;
    private String jfsAccessKey;
    private String jfsSecretKey;
    private String jfsEndPoint;
    private String sparkSql;
    // 输出属性
    private Long audienceSize;

    //人群生成的下限值

    private Long limitSize;
    // 人群包最大值
    private String audienceLimitNum;
    private String audienceType;

    private Boolean ifLimit;
    private AudienceSourceTypeEnum rfmType;

    public String getAudienceId() {
        return audienceId;
    }

    public void setAudienceId(String audienceId) {
        this.audienceId = audienceId;
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

    public String getInputFilePath() {
        return inputFilePath;
    }

    public void setInputFilePath(String inputFilePath) {
        this.inputFilePath = inputFilePath;
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

    public String getSparkSql() {
        return sparkSql;
    }

    public void setSparkSql(String sparkSql) {
        this.sparkSql = sparkSql;
    }

    public Long getAudienceSize() {
        return audienceSize;
    }

    public void setAudienceSize(Long audienceSize) {
        this.audienceSize = audienceSize;
    }

    public Long getLimitSize() {
        return limitSize;
    }

    public void setLimitSize(Long limitSize) {
        this.limitSize = limitSize;
    }

    public String getAudienceLimitNum() {
        return audienceLimitNum;
    }

    public void setAudienceLimitNum(String audienceLimitNum) {
        this.audienceLimitNum = audienceLimitNum;
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

    public AudienceSourceTypeEnum getRfmType() {
        return rfmType;
    }

    public void setRfmType(AudienceSourceTypeEnum rfmType) {
        this.rfmType = rfmType;
    }

    @Override
    public void setStepClassName(String stepClassName) {
        super.setStepClassName("com.jd.easy.audience.task.plugin.step.audience.RfmAudienceGenerateStep");
    }
}

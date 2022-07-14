package com.jd.easy.audience.task.commonbean.bean;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.jd.easy.audience.task.driven.step.StepCommonBean;

import java.util.List;
import java.util.Map;

/**
 * 人群拆分的定义
 *
 * @author cdxiongmei
 * @version 1.0
 */
@JsonTypeName(value = "audienceSplitBean")
public class AudienceSplitBean extends StepCommonBean<Map<String, Object>> {
    private String endPoint;
    private String bucket;
    private String accessKey;
    private String secretKey;
    private String audienceDataFile;
    /**
     * 拆分后子包的定义
     */
    private List<CommonSubPackageBean> subPackages;

    public List<CommonSubPackageBean> getSubPackages() {
        return subPackages;
    }

    public void setSubPackages(List<CommonSubPackageBean> subPackages) {
        this.subPackages = subPackages;
    }

    public String getEndPoint() {
        return endPoint;
    }

    public void setEndPoint(String endPoint) {
        this.endPoint = endPoint;
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

    public String getAudienceDataFile() {
        return audienceDataFile;
    }

    public void setAudienceDataFile(String audienceDataFile) {
        this.audienceDataFile = audienceDataFile;
    }
    @Override
    public void setStepClassName(String stepClassName) {
        super.setStepClassName("com.jd.easy.audience.task.plugin.step.audience.AudienceSplitStep");
    }

}

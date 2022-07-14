package com.jd.easy.audience.task.commonbean.bean;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.jd.easy.audience.common.audience.AudienceSourceTypeEnum;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * 人群圈选中标记数据来源
 *
 * @author cdxiongmei
 * @version 1.0
 */
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public class AudienceDataSourceBean implements Serializable {
    //audienceId or datasetId
    @JsonProperty
    protected String sourceId;
    @JsonProperty
    protected String bucketName;
    @JsonProperty
    protected String endPoint;
    @JsonProperty
    protected String secretKey;
    @JsonProperty
    protected String accessKey;
    /**
     * 格式：ossPath/fileName
     */
    @JsonProperty
    protected String packageUrl;
    @JsonProperty
    protected AudienceSourceTypeEnum sourceType;

    /**
     * 用户标示类型（加密类型和标示类型数值编码）
     */
    private Long userIdType;

    @Override
    public String toString() {
        return "AudienceDataSourceBean{" +
                ", sourceId='" + sourceId + '\'' +
                ", bucket='" + bucketName + '\'' +
                ", endPoint='" + endPoint + '\'' +
                ", secretKey='" + secretKey + '\'' +
                ", accessKey='" + accessKey + '\'' +
                ", sourceType='" + sourceType + '\'' +
                '}';
    }
}

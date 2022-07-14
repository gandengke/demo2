package com.jd.easy.audience.task.commonbean.bean;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.jd.easy.audience.common.constant.LabelSourceEnum;
import com.jd.easy.audience.common.constant.LabelTypeEnum;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 统一标签字段说明
 *
 * @author cdxiongmei
 * @version 1.0
 */
@JsonTypeName(value = "commonLabelSetInfolBean")
public class CommonLabelSetInfolBean implements Serializable {
    /*
     * 标签id
     */
    private Long datasetId;
    /*
     * 标签名称
     */
    private String sourceUrl;
    /*
     * 标签来源
     */
    private LabelSourceEnum labelSource;
    /*
     * 标签枚举信息
     */
    private List<CommonLabelValueDetailBean> labelValueDetails;
    /*
     * 统一标签的setid值
     */
    private String setId;
    /*
     * 标签数据集id类型_id加密类型
     */
    private String idType;

    public String getIdType() {
        return idType;
    }

    public void setIdType(String idType) {
        this.idType = idType;
    }

    public String getSetId() {
        return setId;
    }

    public void setSetId(String setId) {
        this.setId = setId;
    }

    public LabelSourceEnum getLabelSource() {
        return labelSource;
    }

    public void setLabelSource(LabelSourceEnum labelSource) {
        this.labelSource = labelSource;
    }

    public Long getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(Long datasetId) {
        this.datasetId = datasetId;
    }

    public String getSourceUrl() {
        return sourceUrl;
    }

    public void setSourceUrl(String sourceUrl) {
        this.sourceUrl = sourceUrl;
    }

    public List<CommonLabelValueDetailBean> getLabelValueDetails() {
        return labelValueDetails;
    }

    public void setLabelValueDetails(List<CommonLabelValueDetailBean> labelValueDetails) {
        this.labelValueDetails = labelValueDetails;
    }
}

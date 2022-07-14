package com.jd.easy.audience.task.commonbean.bean;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.jd.easy.audience.common.constant.LabelSourceEnum;
import com.jd.easy.audience.common.constant.LabelTypeEnum;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Map;

/**
 * 统一标签字段说明
 *
 * @author cdxiongmei
 * @version 1.0
 */
@Getter
@Setter
@ToString
@JsonTypeName(value = "commonLabelValueDetailBean")
public class CommonLabelValueDetailBean implements Serializable {
    /*
     * 标签id
     */
    private Long labelId;
    /*
     * 标签名称
     */
    private String labelName;
    /*
     * 标签类型
     */
    private LabelTypeEnum labelType;
    private String tab;
    private String tabValue;
    private String type;
    private String isDefault;
    private String isRotate;
    /*
     * 标签枚举信息
     */
    private Map<Long, String> valueMapInfo;

    public String getLabelName() {
        return labelName;
    }

    public void setLabelName(String labelName) {
        this.labelName = labelName;
    }

    public LabelTypeEnum getLabelType() {
        return labelType;
    }

    public void setLabelType(LabelTypeEnum labelType) {
        this.labelType = labelType;
    }

    public Long getLabelId() {
        return labelId;
    }

    public void setLabelId(Long labelId) {
        this.labelId = labelId;
    }

    public Map<Long, String> getValueMapInfo() {
        return valueMapInfo;
    }

    public void setValueMapInfo(Map<Long, String> valueMapInfo) {
        this.valueMapInfo = valueMapInfo;
    }
}

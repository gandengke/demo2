package com.jd.easy.audience.task.commonbean.bean;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.jd.easy.audience.common.constant.LabelSourceEnum;
import com.jd.easy.audience.common.constant.LabelTypeEnum;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * 统一标签字段说明
 *
 * @author cdxiongmei
 * @version 1.0
 */
@Setter
@Getter
@JsonTypeName(value = "commonLabelDetailBean")
public class CommonLabelDetailBean implements Serializable {
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
    /*
     * 标签来源
     */
    private LabelSourceEnum labelSource;
    /*
     * 标签来源地址oss or hive
     */
//    private String sourceUrl;
    /*
     * 统一标签的setid值
     */
    private String setId;

}

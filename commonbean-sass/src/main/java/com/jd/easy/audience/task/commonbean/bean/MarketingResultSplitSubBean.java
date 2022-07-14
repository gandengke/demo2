package com.jd.easy.audience.task.commonbean.bean;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.jd.easy.audience.common.constant.MarketingResultSplitEnum;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * 营销明细-结果分支
 *
 * @author cdxiongmei
 * @version 1.0
 */
@Getter
@Setter
@ToString
@JsonTypeName(value = "marketingResultSplitSubBean")
public class MarketingResultSplitSubBean implements Serializable {
    /*
     * 人群id
     */
    private Long audienceId;
    /*
     * 文件路径不包含文件名
     */
    private String jfsFilePath;
    /*
     * 人群包名称
     */
    private String fileName;
    /*
    * 拆分状态（多种状态使用逗号分隔）
     */
    private String filterRules;
    /**
     * 拆分类型diff or intersection
     */
    private MarketingResultSplitEnum filterType;

}

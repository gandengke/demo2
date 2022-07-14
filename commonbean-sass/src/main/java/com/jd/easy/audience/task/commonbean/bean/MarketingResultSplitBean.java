package com.jd.easy.audience.task.commonbean.bean;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.jd.easy.audience.task.driven.step.StepCommonBean;

import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 人群拆分的定义
 *
 * @author cdxiongmei
 * @version 1.0
 */
@Getter
@Setter
@ToString
@JsonTypeName(value = "marketingResultSplitBean")
public class MarketingResultSplitBean extends StepCommonBean<Map<String, Object>> {
    private String endPoint;
    private String businessId;
    private String source;
    private String bucket;
    private String accessKey;
    private String secretKey;
    private String audienceDataFile;
    /**
     * 拆分后子包的定义
     */
    private List<MarketingResultSplitSubBean> subPackages;

    @Override
    public void setStepClassName(String stepClassName) {
        super.setStepClassName("com.jd.easy.audience.task.plugin.step.audience.MarketingResultSplitStep");
    }

}

package com.jd.easy.audience.task.commonbean.segment;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.jd.easy.audience.task.driven.segment.Segment;
import com.jd.easy.audience.task.driven.step.StepCommonBean;

import java.util.Map;

/**
 * spark的step使用此segment封装
 *
 * @author cdxiongmei
 * @version 1.0
 */
@JsonTypeName(value = "sparkStepSegment")
public class SparkStepSegment extends Segment {

    private Map<String, StepCommonBean> beans;

    private Map<String, String> extraConfig;
    private Map<String, String> extraDeal;
    public Map<String, StepCommonBean> getBeans() {
        return beans;
    }

    public void setBeans(Map<String, StepCommonBean> beans) {
        this.beans = beans;
    }

    public Map<String, String> getExtraConfig() {
        return extraConfig;
    }

    public void setExtraConfig(Map<String, String> extraConfig) {
        this.extraConfig = extraConfig;
    }

    public Map<String, String> getExtraDeal() {
        return extraDeal;
    }

    public void setExtraDeal(Map<String, String> extraDeal) {
        this.extraDeal = extraDeal;
    }

    @Override
    public void validate() {
        if (beans == null || beans.size() == 0) {
            throw new RuntimeException("Steps must be defined");
        }

        if (extraConfig != null && !extraConfig.isEmpty()) {
            for (String key : extraConfig.keySet()) {
                if (!key.startsWith("spark")) {
                    throw new RuntimeException("extra config key must start with 'spark'");
                }
            }
        }
    }
}

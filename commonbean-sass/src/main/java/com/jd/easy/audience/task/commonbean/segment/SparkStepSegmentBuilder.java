package com.jd.easy.audience.task.commonbean.segment;

import com.jd.easy.audience.common.util.JsonUtil;
import com.jd.easy.audience.task.driven.step.StepCommonBean;

import java.util.Map;

/**
 * Spark Segment Builder
 *
 * @author cdxiongmei
 * @version 1.0
 */
public class SparkStepSegmentBuilder {

    // sparkSegment
    private SparkStepSegment sparkStepSegment;

    public SparkStepSegmentBuilder() {
        this.sparkStepSegment = new SparkStepSegment();
    }

    //set app name
    public SparkStepSegmentBuilder setName(String name) {
        this.sparkStepSegment.setName(name);
        return this;
    }

    //set steps
    public SparkStepSegmentBuilder setBeans(Map<String, StepCommonBean> beans) {
        this.sparkStepSegment.setBeans(beans);
        return this;
    }

    //set extra spark config
    public SparkStepSegmentBuilder setExtraConfig(Map<String, String> extraConfig) {
        this.sparkStepSegment.setExtraConfig(extraConfig);
        return this;
    }

    //返回SparkSegment对象
    public SparkStepSegment build() {
        return this.sparkStepSegment;
    }

    // 返回SparkSegment转化成json后的string值
    public String buildAsString() {
        return JsonUtil.serialize(this.sparkStepSegment);
    }
}

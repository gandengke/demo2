package com.jd.easy.audience.task.dataintegration.util.split;

import com.jd.easy.audience.task.commonbean.segment.SparkStepSegment;
import com.jd.easy.audience.task.driven.step.StepCommonBean;

import java.io.IOException;
import java.util.Map;

/**
 * 拆分模块的前置处理
 *
 * @author cdxiongmei
 * @version V1.0
 */
public interface IBeansMapBuilding {
    public Map<String, StepCommonBean> builtBeans(SparkStepSegment sparkSegment, String accountId, String accountName, String dbName) throws IOException;
}

package com.jd.easy.audience.task.commonbean.bean;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.jd.easy.audience.common.model.UddModel;
import com.jd.easy.audience.task.driven.step.StepCommonBean;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * 4a数据集明细任务参数的定义
 *
 * @author cdxiongmei
 * @version 1.0
 */
@Getter
@Setter
@JsonTypeName(value = "uddStatusBean")
public class UddStatusBean extends StepCommonBean<Map<String, Object>> {
    private String userIdField;
    private String statDate;
    private UddModel uddModel;
    private String endPoint;
    private String bucket;
    private String accessKey;
    private String secretKey;
    private String ossFilePath;
    /**
     * 是否使用oneid进行关联
     */
    private boolean useOneId;
    @Override
    public void setStepClassName(String stepClassName) {
        super.setStepClassName("com.jd.easy.audience.task.plugin.step.dataset.udd.UddStatusGeneratorStep");
    }
}

package com.jd.easy.audience.task.commonbean.bean;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.jd.easy.audience.common.model.UddModel;
import com.jd.easy.audience.task.driven.step.StepCommonBean;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * 4a历史日期时间段补数据
 *
 * @author cdxiongmei
 * @version 1.0
 */
@Setter
@Getter
@JsonTypeName(value = "UddBatchBean")
public class UddBatchBean extends StepCommonBean<Map<String, Object>> {
    private boolean useOneId;
    private  String endDate;
    private  String  ossFilePath;
    private  String  esNode;
    private  String  esPort;
    private  String  esassetsIndex;
    private  String  eschainIndex;
    private  String  esUser;
    private  String  esPassword;
    private  String  period;
    private String statDate;
    private UddModel uddModel;
    private String endPoint;
    private String bucket;
    private String accessKey;
    private String secretKey;
    @Override
    public void setStepClassName(String stepClassName) {
        super.setStepClassName("com.jd.easy.audience.task.plugin.step.dataset.udd.UddStatusGeneratorStepTest");
    }
}

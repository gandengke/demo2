package com.jd.easy.audience.task.commonbean.bean;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.jd.easy.audience.task.driven.step.StepCommonBean;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;
import java.util.Map;

/**
 * 人群上传的定义
 *
 * @author cdxiongmei
 * @version 1.0
 */
@Getter
@Setter
@ToString
@JsonTypeName(value = "audiencePortraitDelBean")
public class AudiencePortraitDelBean extends StepCommonBean<Map<String, String>> {
    private String portraitId;

    @Override
    public void setStepClassName(String stepClassName) {
        super.setStepClassName("com.jd.easy.audience.task.plugin.step.audience.AudiencePortraitDelStep");
    }

}

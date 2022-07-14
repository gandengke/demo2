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
@JsonTypeName(value = "audiencePortraitProfileBean")
public class AudiencePortraitProfileBean extends StepCommonBean<Map<String, String>> {
    private String endpoint;
    private String bucket;
    private String accessKey;
    private String secretKey;
    private String audienceDataFile;
    private String outputPath;
    private String audienceId;
    private String portraitId;
    // 直接求交人群/组合求交人群
    private String type;

    /**
     * 进行人群透视的标签数据集信息
     */
    private List<CommonLabelSetInfolBean> labelSets;

    @Override
    public void setStepClassName(String stepClassName) {
        super.setStepClassName("com.jd.easy.audience.task.plugin.step.audience.AudiencePortraitDetailStepV2");
    }

}

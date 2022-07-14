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
@JsonTypeName(value = "audienceCommonProfileBean")
public class AudienceCommonProfileBean extends StepCommonBean<Void> {
    private String endpoint;
    private String bucket;
    private String accessKey;
    private String secretKey;
    private String esNode;
    private String esPort;
    private String esUser;
    private String esPass;
    private String esIndex;
    private String audienceDataFile;
    private Long audienceId;
    /**
     * 统一标签集对应的表信息
     */
    private Map<Long, String> commonLabelTables;
    /**
     * 进行人群透视的标签信息
     */
    private Map<Long, List<CommonLabelDetailBean>> labelDetails;

    @Override
    public void setStepClassName(String stepClassName) {
        super.setStepClassName("com.jd.easy.audience.task.plugin.step.audience.AudienceCommonProfileStep");
    }

}

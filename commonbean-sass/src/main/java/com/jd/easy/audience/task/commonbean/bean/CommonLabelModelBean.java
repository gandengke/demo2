package com.jd.easy.audience.task.commonbean.bean;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.jd.easy.audience.task.driven.step.StepCommonBean;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

/**
 * 统一标签模型参数的定义
 *
 * @author cdxiongmei
 * @version 1.0
 */
@Getter
@Setter
@JsonTypeName(value = "commonLabelModelBean")
public class CommonLabelModelBean extends StepCommonBean<Map<String, Object>> {
    private String esNode;
    private String esPort;
    private String esIndex;
    private String esUser;
    private String esPassword;
    /*
     * 标签信息
     */
    private String labelTable;
    /*
     * 数据集id
     */
    private String setId;
    /**
     * 数据集id
     */
    private Long datasetId;
    /*
     * oss 配置
     */
    private String endpoint;
    /*
     * oss 配置
     */
    private String bucket;
    /*
     * oss 配置
     */
    private String accessKey;
    /*
     * oss 配置
     */
    private String secretKey;
    /**
     * 枚举路径
     */
    private String tagItemPath;
    /*
     * 标签配置信息
     */
    private List<CommonLabelDetailBean> labelDetails;

    public List<CommonLabelDetailBean> getLabelDetails() {
        return labelDetails;
    }

    @Override
    public void setStepClassName(String stepClassName) {
        super.setStepClassName("com.jd.easy.audience.task.plugin.step.dataset.labeldataset.CommonLabelModelStep");
    }
}

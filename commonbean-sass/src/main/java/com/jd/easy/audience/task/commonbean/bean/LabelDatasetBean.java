package com.jd.easy.audience.task.commonbean.bean;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.jd.easy.audience.common.model.param.RelDatasetModel;
import com.jd.easy.audience.task.driven.step.StepCommonBean;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

/**
 * 标签数据集的定义
 *
 * @author cdxiongmei
 * @version 1.0
 */
@Setter
@Getter
@JsonTypeName(value = "labelDatasetBean")
public class LabelDatasetBean extends StepCommonBean<Map<String, Object>> {
    private String userIdField;
    private String userType;
    /**
     * 是否使用oneid进行关联
     */
    private boolean useOneId;
    private List<RelDatasetModel> dataSource;
    private List<LabelDetailBean> labels;
    private Long datasetId;
    private String endpoint;
    private String bucket;
    private String accessKey;
    private String secretKey;
    private String ossFilePath;
    private String tagItemPath;
    private String fileName;
    private String esNode;
    private String esPort;
    private String esIndex;
    private String esUser;
    private String esPassword;

    @Override
    public void setStepClassName(String stepClassName) {
        super.setStepClassName("com.jd.easy.audience.task.plugin.step.dataset.labeldataset.LabelDatasetStep");
    }
}

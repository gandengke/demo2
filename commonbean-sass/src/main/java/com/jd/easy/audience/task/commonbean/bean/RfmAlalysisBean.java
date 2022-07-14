package com.jd.easy.audience.task.commonbean.bean;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.jd.easy.audience.common.constant.RFMDataTypeEnum;
import com.jd.easy.audience.common.model.param.RelDataSetMode;
import com.jd.easy.audience.task.driven.step.StepCommonBean;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * rfm分析参数的定义
 *
 * @author cdxiongmei
 * @version 1.0
 */
@Getter
@Setter
@JsonTypeName(value = "rfmAlalysisBean")
public class RfmAlalysisBean extends StepCommonBean<Void> {
//    private boolean useOneId;
    private RfmAnalysisConfig rfmAnalysisConfig;
    private Long datasetId;
    private String userIdField;
    /**
     * 是否使用oneid进行关联
     */
    private boolean useOneId;
    //deal
    private String saleIdField;
    private String orderDtField;
    //user
    private String recentyField;
    private String frequencyField;

    private RelDataSetMode relDataset;

    private String montaryField;
    private RFMDataTypeEnum dataType;
    /**
     * 明细数据parquet文件路径：labeldatasets/272_dataset_det--不是实际的objectKey，仍需要加parquet
     */
    private String outputPath;
    private int periodDays;
    private List<RfmExFieldDetail> fieldsConfig;
    private String endDate;

    @Override
    public void setStepClassName(String stepClassName) {
        super.setStepClassName("com.jd.easy.audience.task.plugin.step.dataset.rfm.RfmDatasetStep");
    }

}

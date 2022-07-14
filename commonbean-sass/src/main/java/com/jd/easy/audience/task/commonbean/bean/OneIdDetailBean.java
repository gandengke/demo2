package com.jd.easy.audience.task.commonbean.bean;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.jd.easy.audience.common.constant.LabelTypeEnum;
import com.jd.easy.audience.task.driven.step.CircleStepCommonBean;
import com.jd.easy.audience.task.driven.step.StepCommonBean;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Map;

/**
 * 标签明细配置
 *
 * @author cdxiongmei
 * @version 1.0
 */
@Getter
@Setter
@JsonTypeName(value = "oneIdDetailBean")
public class OneIdDetailBean extends CircleStepCommonBean {
//    private String endpoint;
//    private String bucket;
//    private String accessKey;
//    private String secretKey;
    //表名
    private String tableName;
    //oneid字段
    private String oneIdField;
    //id字段
    private String idField;
    //用户标示类型说明
    private String id_type;
    @Override
    public void setStepClassName(String stepClassName) {
        super.setStepClassName("com.jd.easy.audience.task.plugin.step.dataset.labeldataset.OneIdDetailStep");
    }
}

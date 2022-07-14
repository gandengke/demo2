package com.jd.easy.audience.task.commonbean.bean;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.jd.easy.audience.task.driven.step.StepCommonBean;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

/**
 * 人群圈选参数的定义
 *
 * @author cdxiongmei
 * @version 1.0
 */
@Getter
@Setter
@JsonTypeName(value = "audienceGenerateBean")
public class AudienceGenerateBean extends StepCommonBean<Map<String, Object>> {
    private String sparkSQL;
    private String audienceId;
    //输出属性
    private Long audienceSize;
    // 人群生成的下限值
    private Long limitSize;
    /**
     * 人群结果的用户标示类型
     */
    private Long userIdType;
    /**
     * 是否使用oneid进行关联
     */
    private boolean useOneId;
    /**
     * 租户关联的库信息
     */
    private String dbName;
    //数据来源配置
    List<AudienceDataSourceBean> dataSource;
    @Override
    public void setStepClassName(String stepClassName) {
        super.setStepClassName("com.jd.easy.audience.task.plugin.step.audience.AudienceGenerateStep");
    }
}

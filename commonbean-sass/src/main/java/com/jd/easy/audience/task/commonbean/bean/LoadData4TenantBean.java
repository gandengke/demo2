package com.jd.easy.audience.task.commonbean.bean;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.jd.easy.audience.task.commonbean.contant.BuffaloCircleTypeEnum;
import com.jd.easy.audience.task.driven.step.CircleStepCommonBean;
import lombok.Data;

/**
 * 帮助crm 租户数据落库
 *
 * @author cdxiongmei
 * @version 1.0
 */
@Deprecated
@Data
@JsonTypeName(value = "loadData4TenantBean")
public class LoadData4TenantBean extends CircleStepCommonBean {
    //CRM具体的表名
    private String sourceTableName;
    //是否需要进行表重建
    private Boolean isRebuilt;
    //数据字段集
    private String fieldFilter;
    /**
     * 是否需要同步表信息到mysql中(one_id的数据不需要更新到mysql中)
     */
    private boolean update2Mysql = true;

    /**
     * 更新周期
     */
    private BuffaloCircleTypeEnum circleType;
    /**
     * 更新步长
     */
    private Integer circleStep;
    //前置过滤条件（ca根据日志更新部分数据）
    private String filterExpress;

    @Override
    public void setStepClassName(String stepClassName) {
        super.setStepClassName("com.jd.easy.audience.task.dataintegration.step.LoadData4TenantStep");
    }
}

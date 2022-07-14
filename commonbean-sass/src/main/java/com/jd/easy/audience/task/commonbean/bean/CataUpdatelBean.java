package com.jd.easy.audience.task.commonbean.bean;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.jd.easy.audience.task.driven.step.CircleStepCommonBean;
import lombok.Data;

/**
 * 元数据更新
 *
 * @author cdxiongmei
 * @version 1.0
 */
@Data
@JsonTypeName(value = "cataUpdatelBean")
public class CataUpdatelBean extends CircleStepCommonBean {
    private Boolean notExistDrop = false;
    /**
     * 是否需要无条件更新
     */
    private Boolean isAllUpdate = false;

    @Override
    public void setStepClassName(String stepClassName) {
        super.setStepClassName("com.jd.easy.audience.task.dataintegration.step.CataTableInfoUpdateTenantStep");
    }
}

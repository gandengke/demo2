package com.jd.easy.audience.task.commonbean.bean;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.jd.easy.audience.task.driven.step.StepCommonBean;

import java.util.Map;

/**
 * 建表任务参数的定义
 *
 * @author cdxiongmei
 * @version 1.0
 */
@JsonTypeName(value = "createTabelBean")
public class CreateTabelBean extends StepCommonBean<Map<String, Object>> {
    private String tableCreateSql;
    private String dbName;
    private String targetTable;
    private Boolean existDrop = false;

    public String getTableCreateSql() {
        return tableCreateSql;
    }

    public void setTableCreateSql(String tableCreateSql) {
        this.tableCreateSql = tableCreateSql;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public Boolean getExistDrop() {
        return existDrop;
    }

    public void setExistDrop(Boolean existDrop) {
        this.existDrop = existDrop;
    }

    @Override
    public void setStepClassName(String stepClassName) {
        super.setStepClassName("com.jd.easy.audience.task.dataintegration.step.CreateHiveTableStep");
    }
}

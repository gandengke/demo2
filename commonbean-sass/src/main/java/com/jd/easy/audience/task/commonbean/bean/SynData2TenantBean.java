package com.jd.easy.audience.task.commonbean.bean;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.jd.easy.audience.common.constant.SplitDataSourceEnum;
import com.jd.easy.audience.common.constant.SplitDataTypeEnum;
import com.jd.easy.audience.task.commonbean.contant.SplitSourceTypeEnum;
import com.jd.easy.audience.task.driven.step.CircleStepCommonBean;
import com.jd.easy.audience.task.driven.step.StepCommonBean;
import lombok.Data;

/**
 * 帮助crm 租户数据直接从同集市其他库中同步
 *
 * @author cdxiongmei
 * @version 1.0
 */
@Data
@JsonTypeName(value = "SynData2TenantBean")
public class SynData2TenantBean extends CircleStepCommonBean {
    //具体的表名
    private String sourceTableName;
    //sourceTable 所在的库名
    private String sourceDbName;
    //各租户库中的表名前缀
    private String subTableName;
    //分区字段
    private String dtFieldSource;
    private SplitDataTypeEnum dataType;
    //前置过滤条件（ca根据日志更新部分数据）
    private String filterExpress;
    //数据来源
    private SplitDataSourceEnum dataSource;
    //数据字段集
    private String fieldFilter;
//    //租户字段
//    private String tenantFieldSource;
    //需要拆分的维度，可能有多个字段，英文逗号分割
    private String splitFieldEx;
    //数据来源-细分
    private SplitSourceTypeEnum sourceTypeEx;
    //字表注释
    private String tableComment;

    /**
     * 是否需要同步表信息到mysql中(one_id的数据不需要更新到mysql中)
     */
    private boolean update2Mysql = true;
    @Override
    public void setStepClassName(String stepClassName) {
        super.setStepClassName("com.jd.easy.audience.task.dataintegration.step.SynData2TenantStep");
    }

    public SplitSourceTypeEnum getSourceTypeEx() {
        return sourceTypeEx;
    }

    public void setSourceTypeEx(SplitSourceTypeEnum sourceTypeEx) {
        this.sourceTypeEx = sourceTypeEx;
    }

    public String getTableComment() {
        return tableComment;
    }

    public void setTableComment(String tableComment) {
        this.tableComment = tableComment;
    }

    public boolean isUpdate2Mysql() {
        return update2Mysql;
    }

    public void setUpdate2Mysql(boolean update2Mysql) {
        this.update2Mysql = update2Mysql;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public String getSourceDbName() {
        return sourceDbName;
    }

    public void setSourceDbName(String sourceDbName) {
        this.sourceDbName = sourceDbName;
    }

    public String getSubTableName() {
        return subTableName;
    }

    public void setSubTableName(String subTableName) {
        this.subTableName = subTableName;
    }

    public String getDtFieldSource() {
        return dtFieldSource;
    }

    public void setDtFieldSource(String dtFieldSource) {
        this.dtFieldSource = dtFieldSource;
    }

    public SplitDataTypeEnum getDataType() {
        return dataType;
    }

    public void setDataType(SplitDataTypeEnum dataType) {
        this.dataType = dataType;
    }

    public String getFilterExpress() {
        return filterExpress;
    }

    public void setFilterExpress(String filterExpress) {
        this.filterExpress = filterExpress;
    }

    public SplitDataSourceEnum getDataSource() {
        return dataSource;
    }

    public void setDataSource(SplitDataSourceEnum dataSource) {
        this.dataSource = dataSource;
    }

    public String getFieldFilter() {
        return fieldFilter;
    }

    public void setFieldFilter(String fieldFilter) {
        this.fieldFilter = fieldFilter;
    }

    public String getSplitFieldEx() {
        return splitFieldEx;
    }

    public void setSplitFieldEx(String splitFieldEx) {
        this.splitFieldEx = splitFieldEx;
    }
}

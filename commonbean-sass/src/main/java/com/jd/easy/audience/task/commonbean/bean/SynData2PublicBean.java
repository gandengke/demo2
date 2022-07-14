package com.jd.easy.audience.task.commonbean.bean;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.jd.easy.audience.common.constant.SplitDataSourceEnum;
import com.jd.easy.audience.common.constant.SplitDataTypeEnum;
import com.jd.easy.audience.task.commonbean.contant.SplitSourceTypeEnum;
import com.jd.easy.audience.task.driven.step.StepCommonBean;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

/**
 * 帮助crm 租户数据同步到cdp集市公共库
 *
 * @author cdxiongmei
 * @version 1.0
 */
@Data
@JsonTypeName(value = "synData2PublicBean")
public class SynData2PublicBean extends StepCommonBean {
    //具体的表名
    private String sourceTableName;
    //目标表名
    private String desTableName;
    //sourceTable 所在的库名
    private String sourceDbName;
    //各租户库中的表名前缀
    private String subTableNamePrefix;

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    //分区字段
    private String dtFieldSource;
    //是否需要进行表重建
    private Boolean isRebuilt;
    private SplitDataTypeEnum dataType;
    //前置过滤条件（ca根据日志更新部分数据）
    private String filterExpress;
    //数据来源
    private SplitDataSourceEnum dataSource;
    //数据字段集
    private String fieldFilter;
    private int dateBefore;
    //租户字段
    private String tenantFieldSource;
    //需要拆分的维度，可能有多个字段，英文逗号分割
    private String splitFieldEx;
    //数据来源-细分
    private SplitSourceTypeEnum sourceTypeEx;
    //字表注释
    private String tableComment;

    @Override
    public void setStepClassName(String stepClassName) {
        super.setStepClassName("com.jd.easy.audience.task.dataintegration.step.SynData2PublicStep");
    }

    public String getDesTableName() {
        return desTableName;
    }

    public void setDesTableName(String desTableName) {
        this.desTableName = desTableName;
    }

    public String getSourceDbName() {
        return sourceDbName;
    }

    public void setSourceDbName(String sourceDbName) {
        this.sourceDbName = sourceDbName;
    }

    public  Boolean getIsRebuilt(){
        return isRebuilt;
    }

    public void setIsRebuilt(boolean isRebuilt){
        this.isRebuilt = isRebuilt;
    }

    public String getSubTableNamePrefix() {
        return subTableNamePrefix;
    }

    public void setSubTableNamePrefix(String subTableNamePrefix) {
        this.subTableNamePrefix = subTableNamePrefix;
    }

    public String getDtFieldSource() {
        return dtFieldSource;
    }

    public void setDtFieldSource(String dtFieldSource) {
        this.dtFieldSource = dtFieldSource;
    }

    public Boolean getRebuilt() {
        return isRebuilt;
    }

    public void setRebuilt(Boolean rebuilt) {
        isRebuilt = rebuilt;
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

    public int getDateBefore() {
        return dateBefore;
    }

    public void setDateBefore(int dateBefore) {
        this.dateBefore = dateBefore;
    }

    public String getTenantFieldSource() {
        return tenantFieldSource;
    }

    public void setTenantFieldSource(String tenantFieldSource) {
        this.tenantFieldSource = tenantFieldSource;
    }

    public String getSplitFieldEx() {
        return splitFieldEx;
    }

    public void setSplitFieldEx(String splitFieldEx) {
        this.splitFieldEx = splitFieldEx;
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
}

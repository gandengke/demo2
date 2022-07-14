package com.jd.easy.audience.task.commonbean.bean;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.jd.easy.audience.common.constant.SplitDataSourceEnum;
import com.jd.easy.audience.common.constant.SplitDataTypeEnum;
import com.jd.easy.audience.task.commonbean.contant.SplitSourceTypeEnum;
import com.jd.easy.audience.task.driven.step.StepCommonBean;

/**
 * 帮助crm 拆分租户数据
 *
 * @author cdxiongmei
 * @version 1.0
 */
@Deprecated
@JsonTypeName(value = "splitDataBean")
public class SplitDataBean extends StepCommonBean {
    //CRM具体的表名
    private String sourceTable;
    //sourceTable 所在的库名
    private String sourceDbName;
    //各租户库中的表名前缀
    private String desTablePre;
    //分区字段
    private String dtFieldSource;
    //租户字段
    private String tenantFieldSource;
    //需要拆分的维度，可能有多个字段，英文逗号分割
    private String splitFieldEx;
    //是否需要进行表重建
    private Boolean isRebuilt;
    private SplitDataTypeEnum dataType;
    //前置过滤条件（ca根据日志更新部分数据）
    private String filterExpress;
    //数据来源
    private SplitDataSourceEnum dataSource;
    //数据来源-细分
    private SplitSourceTypeEnum sourceTypeEx;
    //数据字段集
    private String fieldFilter;
    //字表注释
    private String tableComment;

    private int dateBefore;
    /**
     * 是否需要同步表信息到mysql中
     */
    private boolean update2Mysql = true;

    public boolean isUpdate2Mysql() {
        return update2Mysql;
    }

    public void setUpdate2Mysql(boolean update2Mysql) {
        this.update2Mysql = update2Mysql;
    }

    public int getDateBefore() {
        return dateBefore;
    }

    public void setDateBefore(int dateBefore) {
        this.dateBefore = dateBefore;
    }

    public String getTableComment() {
        return tableComment;
    }

    public void setTableComment(String tableComment) {
        this.tableComment = tableComment;
    }

    public String getFieldFilter() {
        return fieldFilter;
    }

    public void setFieldFilter(String fieldFilter) {
        this.fieldFilter = fieldFilter;
    }

    public SplitSourceTypeEnum getSourceTypeEx() {
        return sourceTypeEx;
    }

    public void setSourceTypeEx(SplitSourceTypeEnum sourceTypeEx) {
        this.sourceTypeEx = sourceTypeEx;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getSourceDbName() {
        return sourceDbName;
    }

    public void setSourceDbName(String sourceDbName) {
        this.sourceDbName = sourceDbName;
    }

    public String getDtFieldSource() {
        return dtFieldSource;
    }

    public void setDtFieldSource(String dtFieldSource) {
        this.dtFieldSource = dtFieldSource;
    }

    public String getTenantFieldSource() {
        return tenantFieldSource;
    }

    public void setTenantFieldSource(String tenantFieldSource) {
        this.tenantFieldSource = tenantFieldSource;
    }

//    public String getDtFieldDes() {
//        return dtFieldDes;
//    }
//
//    public void setDtFieldDes(String dtFieldDes) {
//        this.dtFieldDes = dtFieldDes;
//    }

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

    public String getDesTablePre() {
        return desTablePre;
    }

    public void setDesTablePre(String desTablePre) {
        this.desTablePre = desTablePre;
    }

    public String getSplitFieldEx() {
        return splitFieldEx;
    }

    public void setSplitFieldEx(String splitFieldEx) {
        this.splitFieldEx = splitFieldEx;
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

    @Override
    public void setStepClassName(String stepClassName) {
        super.setStepClassName("com.jd.easy.audience.task.dataintegration.step.SplitDataStep");
    }
}

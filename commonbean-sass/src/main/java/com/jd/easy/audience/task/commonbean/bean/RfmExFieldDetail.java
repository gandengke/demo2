package com.jd.easy.audience.task.commonbean.bean;

import java.io.Serializable;

/**
 * rfm扩展字段定义
 *
 * @author cdxiongmei
 * @version 1.0
 */
public class RfmExFieldDetail implements Serializable {
   //标签id
    private Long fieldId;
    //数据库
    private String database;
    //表
    private String table;
    //字段
    private String fieldName;
    //字段
    private String fieldExName;
    // 分区字段

    private String dtField;

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Long getFieldId() {
        return fieldId;
    }

    public void setFieldId(Long fieldId) {
        this.fieldId = fieldId;
    }
    public String getFieldExName() {
        return fieldExName;
    }

    public void setFieldExName(String fieldExName) {
        this.fieldExName = fieldExName;
    }

    public String getDtField() {
        return dtField;
    }

    public void setDtField(String dtField) {
        this.dtField = dtField;
    }

}

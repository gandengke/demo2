package com.jd.easy.audience.task.commonbean.bean;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.jd.easy.audience.common.oss.OssFileTypeEnum;
import com.jd.easy.audience.task.driven.step.StepCommonBean;

import java.util.Map;

/**
 * 数据上传参数的定义
 *
 * @author cdxiongmei
 * @version 1.0
 */
@JsonTypeName(value = "oss2HiveBean")
public class Oss2HiveBean extends StepCommonBean<Map<String, Object>> {
    private String bucket;
    private String endPoint;
    private String accessKey;
    private String secretKey;
    //osspath/filename or osspath(支持文件和目录)
    private String filePath;
    private String dbName;
    private String targetTable;
    private OssFileTypeEnum fileType;
    private Boolean overWrite = false;
    private String encoding;

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getEndPoint() {
        return endPoint;
    }

    public void setEndPoint(String endPoint) {
        this.endPoint = endPoint;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
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

    public OssFileTypeEnum getFileType() {
        return fileType;
    }

    public void setFileType(OssFileTypeEnum fileType) {
        this.fileType = fileType;
    }

    public Boolean getOverWrite() {
        return overWrite;
    }

    public void setOverWrite(Boolean overWrite) {
        this.overWrite = overWrite;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    @Override
    public void setStepClassName(String stepClassName) {
        super.setStepClassName("com.jd.easy.audience.task.dataintegration.step.OssToHiveStep");
    }
}

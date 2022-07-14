package com.jd.easy.audience.task.commonbean.bean;

import com.fasterxml.jackson.annotation.JsonTypeName;

import java.io.Serializable;

/**
 * 人群拆分子包定义-随机分支
 *
 * @author cdxiongmei
 * @version 1.0
 */
@JsonTypeName(value = "commonSubPackageBean")
public class CommonSubPackageBean implements Serializable {
    /*
     * 人群id
     */
    private Long audienceId;
    /*
     * 人群大小
     */
    private Long audienceSize;

    /*
     * 文件路径不包含文件名
     */
    private String jfsFilePath;
    /*
     * 人群包名称
     */
    private String fileName;
    /*
    * 拆分的比例（小于1）
     */
    private Double ratio;

    public Long getAudienceId() {
        return audienceId;
    }

    public void setAudienceId(Long audienceId) {
        this.audienceId = audienceId;
    }

    public Long getAudienceSize() {
        return audienceSize;
    }

    public void setAudienceSize(Long audienceSize) {
        this.audienceSize = audienceSize;
    }

    public String getJfsFilePath() {
        return jfsFilePath;
    }

    public void setJfsFilePath(String jfsFilePath) {
        this.jfsFilePath = jfsFilePath;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public Double getRatio() {
        return ratio;
    }

    public void setRatio(Double ratio) {
        this.ratio = ratio;
    }
}

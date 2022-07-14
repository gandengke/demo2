package com.jd.easy.audience.task.commonbean.bean;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.jd.easy.audience.task.commonbean.contant.BuffaloCircleTypeEnum;
import com.jd.easy.audience.task.driven.step.CircleStepCommonBean;
import lombok.Data;

/**
 * 元数据更新
 *
 * @author cdxiongmei
 * @version 1.0
 */
@Data
@JsonTypeName(value = "hdfs2HiveBean")
public class Hdfs2HiveBean extends CircleStepCommonBean {
    /**
     * hdfs数据扩展路径
     */
    private String extendPath;
    /**
     * 目标hive表名
     */
    private String tableName;
    /**
     * 填写字段名
     */
    private String fieldList;
    /**
     * 消息类型 json/csv/stream/dts
     */
    private String msgType;

    /**
     * 字段是否需要驼峰处理
     */
    private Boolean isHump;
    /**
     * 是否通过数据库区分目录（dts是通过数据库区分目录，其他的使用租户id区分）
     */
    private Boolean isDirDb;
    /**
     * 更新周期
     */
    private BuffaloCircleTypeEnum circleType;
    /**
     * 更新步长
     */
    private Integer circleStep;
    @Override
    public void setStepClassName(String stepClassName) {
        super.setStepClassName("com.jd.easy.audience.task.dataintegration.step.Hdfs2HiveProcessStep");
    }
}

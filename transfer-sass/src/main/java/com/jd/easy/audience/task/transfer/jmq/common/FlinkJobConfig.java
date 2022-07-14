package com.jd.easy.audience.task.transfer.jmq.common;

import lombok.Data;

import java.io.Serializable;

/**
 * flink JOB 配置
 *
 * @author cdxiongmei
 * @version V1.0
 */
@Data
public class FlinkJobConfig implements Serializable {
    /**
     * topic
     */
    private String appId;
    private String topicName;
    /**
     * 消息类型
     */
    private String msgType;
    /**
     * 生产账号名称
     */
    private String accountName;
    /**
     * 扩展路径关联的字段
     */
    private String extendPathField;
    /**
     * 用于封装json的字段名
     */
    private String fieldsSet;

}

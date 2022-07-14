package com.jd.easy.audience.task.commonbean.contant;

import lombok.Getter;

/**
 * 需要按租户拆分数据的模版源数据类型
 *
 * @author cdxiongmei
 * @version 1.0
 */
public enum BuffaloCircleTypeEnum {
    //json
    DAY("DAY"),
    HOUR("HOUR"),
    MINUTE("MINUTE");

    //消息类型
    @Getter
    private String value;

    BuffaloCircleTypeEnum(String value) {
        this.value = value;
    }
}

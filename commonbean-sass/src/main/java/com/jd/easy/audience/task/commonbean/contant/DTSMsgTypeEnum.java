package com.jd.easy.audience.task.commonbean.contant;

import lombok.Getter;
import lombok.Setter;

/**
 * 需要按租户拆分数据的模版源数据类型
 *
 * @author cdxiongmei
 * @version 1.0
 */
public enum DTSMsgTypeEnum {
    //json
    JSON("JSON"),
    DTS("DTS"),
    CSVSTREAM("CSVSTREAM");

    //消息类型
    @Getter
    private String value;

    DTSMsgTypeEnum(String value) {
        this.value = value;
    }
}

package com.jd.easy.audience.task.commonbean.bean;

import com.jd.easy.audience.common.constant.LabelTypeEnum;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * 标签明细配置
 *
 * @author cdxiongmei
 * @version 1.0
 */
@Getter
@Setter
public class LabelDetailBean implements Serializable {
    //标签id
    private Long labelId;

    //数据库
    private String database;
    //表
    private String table;
    //字段
    private String fieldName;
    //关联字段
//    private String relationField;
    //标签字段类型
    private LabelTypeEnum labelType;
    //标签值的解析分隔符
    private String fieldDelimiter;
}

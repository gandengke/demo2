package com.jd.easy.audience.task.commonbean.contant;

import org.apache.commons.lang.StringUtils;

/**
 * 需要按租户拆分数据的模版源数据类型
 *
 * @author cdxiongmei
 * @version 1.0
 */
public enum CATemplateEnum {
    //点击数据
    CLICKLOG("clicklog", "点击日志模版", true, "event_date", "mediaData"),
    //曝光数据
    IMPRESSLOG("impresslog", "展现日志模版", true, "event_date", "mediaData"),
    //会员交易
    MEMBERTRADE("membertrade", "会员交易信息", true, "event_date", "memberData"),
    //会员信息
    MEMBERINFORMATION("memberinformation", "会员信息", false, null, "memberData"),
    //用户行为
    USERBEHAVIR("userbehavior", "用户行为数据", true, "event_date","behaviorData"),
    //用户交易
    USERTRADE("usertrade", "用户交易统计", true, "event_date","behaviorData"),
    //用户信息
    USERINFORMATION("userinformation", "用户信息模板", false, null,"behaviorData"),
    //用户订单
    USERORDER("userorder", "用户订单信息", true, "event_date","behaviorData"),
    HOUSE("house", "商品信息房产", false, null,"behaviorData"),
    CAR("car", "房产信息汽车", false, null,"behaviorData"),
    ECOMMERCE("ecommerce", "商品信息电商", false, null,"behaviorData");

    //模版id
//    private String dbName;
    //模版对应的表
    private String templateTable;
    //说明
    private String desc;
    //每个模版的拆分类型
    private boolean isPartition;
    //用于分区的日期字段
    private String partitionField;
    /**
     * okr数据类型
     */
    private String dataType;

    CATemplateEnum(String templateTable, String desc, boolean isPartition, String partitionField, String dataType) {
        this.templateTable = templateTable;
        this.desc = desc;
        this.isPartition = isPartition;
        this.partitionField = partitionField;
        this.dataType = dataType;
    }

    public boolean isPartition() {
        return isPartition;
    }

    public String getPartitionField() {
        return partitionField;
    }

    public String getDesc() {
        return desc;
    }

    public String getTemplateTable() {
        return templateTable;
    }

    public String getDataType() {
        return dataType;
    }

    /**
     * 根据ca的模版id获取拆分信息
     *
     * @param tableName 模板名
     * @return 模版对应的配置信息
     */
    public static CATemplateEnum getCATemplateEnum(String tableName) {
        if (StringUtils.isBlank(tableName)) {
            return null;
        }
        CATemplateEnum[] enums = CATemplateEnum.values();
        for (CATemplateEnum enumEle : enums) {
            if (enumEle.getTemplateTable().equals(tableName)) {
                return enumEle;
            }
        }
        return null;
    }
}

package com.jd.easy.audience.task.commonbean.contant;

/**
 * 拆分数据的数据来源
 *
 * @author cdxiongmei
 * @version 1.0
 */
public enum SplitSourceTypeEnum {
    //cdp系统中自定义创建
    CUSTOM(1, "CUSTOM"),
    //文件上传
    FILEUPLOAD(2, "UPLOAD"),
    //站点监测
    SITEDETECTION(4, "SITEDETECTION"),
    //广告监测
    ADDETECTION(5, "ADDETECTION"),
    //黒珑监测
    HEILONGDETECTION(6, "HEILONGDETECTION"),
    //crm使用
    OFFLINETASK(7, "OFFLINETASK"),
    //bdp创建的表
    BDPTABLE(8, "BDP"),
    /** 企微同步 */
    ENTERPRISE_WECHAT(9, "企微同步"),
    /** 微信公众号同步 */
    WECHAT_OFFICIAL_ACCOUNT(10, "微信公众号同步"),
    //api接入
    PULL(3, "PULL"),
    PUSH(3, "PUSH"),
    UNKNOWN(100, "UNKNOWN");
    private int type;
    private String desc;

    SplitSourceTypeEnum(int type, String desc) {
        this.type = type;
        this.desc = desc;
    }

    public int getType() {
        return type;
    }

    public String getDesc() {
        return desc;
    }

    public static SplitSourceTypeEnum getTypeEnum(String desc) {
        SplitSourceTypeEnum[] enums = SplitSourceTypeEnum.values();
        for (SplitSourceTypeEnum enumEle : enums) {
            if (enumEle.getDesc().equals(desc)) {
                return enumEle;
            }
        }
        return UNKNOWN;
    }
    public static SplitSourceTypeEnum getTypeEnumByType(int type) {
        SplitSourceTypeEnum[] enums = SplitSourceTypeEnum.values();
        for (SplitSourceTypeEnum enumEle : enums) {
            if (enumEle.getType() == type) {
                return enumEle;
            }
        }
        return UNKNOWN;
    }
}

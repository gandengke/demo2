package com.jd.easy.audience.task.commonbean.contant;

/**
 * 拆分数据的数据来源
 *
 * @author cdxiongmei
 * @version 1.0
 */
public enum DepartmentTypeEnum {
    //根据数据源查询部门
    DATA_SOURCE(1, "source_id", "根据数据源查询部门"),
    //数据回流-站点数据,根据数据源查询部门
    SITE_DATE(2, "site_id", "数据回流-站点数据,根据数据源查询部门"),
    //数据回流-监测数据,根据数据源查询部门
    MONITOR_DATA(3, "ad_plan_id", "数据回流-监测数据,根据数据源查询部门"),
    //数据回流-黑珑监测,根据数据源查询部门
    HEILONG_MONITOR_DATA(4, "advertiser_id", "数据回流-黑珑监测,根据数据源查询部门");
    private Integer type;
    private String sourceField;
    private String desc;

    DepartmentTypeEnum(Integer type, String sourceField, String desc) {
        this.type = type;
        this.sourceField = sourceField;
        this.desc = desc;
    }

    public Integer getType() {
        return type;
    }

    public String getDesc() {
        return desc;
    }

    public String getSourceField() {
        return sourceField;
    }

    public static DepartmentTypeEnum getTypeEnum(String desc) {
        DepartmentTypeEnum[] enums = DepartmentTypeEnum.values();
        for (DepartmentTypeEnum enumEle : enums) {
            if (enumEle.getDesc().equals(desc)) {
                return enumEle;
            }
        }
        return null;
    }
    public static DepartmentTypeEnum getTypeEnumByType(Integer type) {
        DepartmentTypeEnum[] enums = DepartmentTypeEnum.values();
        for (DepartmentTypeEnum enumEle : enums) {
            if (enumEle.getType().intValue() == type.intValue()) {
                return enumEle;
            }
        }
        return null;
    }

    public static DepartmentTypeEnum getTypeEnumBySourceType(SplitSourceTypeEnum sourceType) {
        if (sourceType.equals(SplitSourceTypeEnum.PUSH) || sourceType.equals(SplitSourceTypeEnum.PULL) || sourceType.equals(SplitSourceTypeEnum.FILEUPLOAD)) {
            return DepartmentTypeEnum.DATA_SOURCE;
        } else if (sourceType.equals(SplitSourceTypeEnum.ADDETECTION)) {
            return  DepartmentTypeEnum.MONITOR_DATA;
        } else if (sourceType.equals(SplitSourceTypeEnum.HEILONGDETECTION)) {
            return  DepartmentTypeEnum.HEILONG_MONITOR_DATA;
        }else if (sourceType.equals(SplitSourceTypeEnum.SITEDETECTION)) {
            return  DepartmentTypeEnum.SITE_DATE;
        } else {
            return null;
        }
    }
}

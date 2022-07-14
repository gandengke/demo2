package com.jd.easy.audience.task.plugin.util.es.sqltool;

import com.jd.easy.audience.common.constant.LogicalOperatorEnum;

/**
 * sql条件，暂不支持嵌套
 *
 * @author one3c-cdyaoxiangyuan
 * @Date 2021/5/11
 * @Time 17:25
 */
public class SqlCondition {
    /** 逻辑操作枚举 */
    private LogicalOperatorEnum logicalOperator;
    /** 字段名称 */
    private String columnName;
    /** 比较类型 */
    private ComparisonOperatorsEnum compareOpType;
    /** 第一个参数 */
    private String param1;
    /** 第二个参数 */
    private String param2;

    public LogicalOperatorEnum getLogicalOperator() {
        return logicalOperator;
    }

    public void setLogicalOperator(LogicalOperatorEnum logicalOperator) {
        this.logicalOperator = logicalOperator;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public ComparisonOperatorsEnum getCompareOpType() {
        return compareOpType;
    }

    public void setCompareOpType(ComparisonOperatorsEnum compareOpType) {
        this.compareOpType = compareOpType;
    }

    public String getParam1() {
        return param1;
    }

    public void setParam1(String param1) {
        this.param1 = param1;
    }

    public String getParam2() {
        return param2;
    }

    public void setParam2(String param2) {
        this.param2 = param2;
    }
}

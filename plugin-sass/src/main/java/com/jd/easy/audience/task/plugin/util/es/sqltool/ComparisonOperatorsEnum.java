package com.jd.easy.audience.task.plugin.util.es.sqltool;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;

/**
 * 比较操作类型
 *
 * @author one3c-cdyaoxiangyuan
 * @Date 2021/5/11
 * @Time 17:29
 */
public enum ComparisonOperatorsEnum {
    /**
     * 等于
     */
    EQUALITY("="),
    /**
     * 不等于
     */
    INEQUALITY("<>", "!=", "<=>"),
    /**
     * 大于
     */
    GT(">"),
    /**
     * 大于等于
     */
    GE(">="),
    /**
     * 小于
     */
    LT("<"),
    /**
     * 小于等于
     */
    LE("<="),
    /**
     * 介于
     */
    BETWEEN("BETWEEN"),
    /**
     * is null
     */
    ISNULL("IS NULL"),
    /**
     * is not null
     */
    ISNOTNULL("IS NOT NULL");

    /**
     * 比较符号
     */
    String[] operator;

    /**
     * construct
     *
     * @param operator
     */
    ComparisonOperatorsEnum(String... operator) {
        this.operator = operator;
    }

    /**
     * 反馈操作符
     *
     * @return
     */
    public String[] getOperator() {
        return operator;
    }

    /**
     * 通过操作类型获取枚举
     *
     * @param op
     * @return
     */
    public static ComparisonOperatorsEnum getByOp(String op) {
        if (StringUtils.isBlank(op)) {
            return null;
        }
        op = StringUtils.trim(op);
        ComparisonOperatorsEnum[] ops = ComparisonOperatorsEnum.values();
        for (ComparisonOperatorsEnum tmp : ops) {
            for (String tmpOp : tmp.operator) {
                if (tmpOp.equalsIgnoreCase(op.trim())) {
                    return tmp;
                }
            }
        }
        return null;
    }

    /**
     * 通过前缀获取操作类型枚举
     *
     * @param cond
     * @return
     */
    public static ComparisonOperatorsEnum getByPrefix(String cond, AtomicInteger... opLength) {
        if (StringUtils.isBlank(cond)) {
            return null;
        }
        cond = StringUtils.trim(cond);
        ComparisonOperatorsEnum[] ops = ComparisonOperatorsEnum.values();
        for (ComparisonOperatorsEnum tmp : ops) {
            for (String tmpOp : tmp.operator) {
                if (StringUtils.startsWithIgnoreCase(cond, tmpOp)) {
                    if (opLength != null && opLength.length > 0 && opLength[0] != null) {
                        opLength[0].set(tmpOp.length());
                    }
                    return tmp;
                }
            }
        }
        return null;
    }
}

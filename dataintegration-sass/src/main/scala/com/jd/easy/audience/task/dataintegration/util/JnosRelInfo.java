package com.jd.easy.audience.task.dataintegration.util;

/**
 * jnos账号hive库关联关系
 *
 * @author cdxiongmei
 * @version V1.0
 */

public class JnosRelInfo {
    /**
     * 组织id
     */
    private Long accountId;
    /**
     * jnos主账号
     */
    private String mainAccountName;
    /**
     * hive对应的过滤字段
     */
    private String filterCode;
    /**
     * jnos子账户名称
     */
    private String subAccountName;
    /**
     * hive数据库id
     */
    private Long dbId;
    /**
     * hive数据库名称
     */
    private String dbName;

    public Long getAccountId() {
        return accountId;
    }

    public void setAccountId(Long accountId) {
        this.accountId = accountId;
    }

    public String getMainAccountName() {
        return mainAccountName;
    }

    public void setMainAccountName(String mainAccountName) {
        this.mainAccountName = mainAccountName;
    }

    public String getFilterCode() {
        return filterCode;
    }

    public void setFilterCode(String filterCode) {
        this.filterCode = filterCode;
    }

    public String getSubAccountName() {
        return subAccountName;
    }

    public void setSubAccountName(String subAccountName) {
        this.subAccountName = subAccountName;
    }

    public Long getDbId() {
        return dbId;
    }

    public void setDbId(Long dbId) {
        this.dbId = dbId;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public JnosRelInfo(Long accountId, String mainAccountName, String filterCode, String subAccountName, Long dbId, String dbName) {
        this.accountId = accountId;
        this.mainAccountName = mainAccountName;
        this.filterCode = filterCode;
        this.subAccountName = subAccountName;
        this.dbId = dbId;
        this.dbName = dbName;
    }
}

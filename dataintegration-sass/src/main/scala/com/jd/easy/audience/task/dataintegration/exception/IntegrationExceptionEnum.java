package com.jd.easy.audience.task.dataintegration.exception;

import org.apache.commons.lang.StringUtils;

public enum IntegrationExceptionEnum {
    /**
     * RFM模型生成
     */
    SPLITDATA_WRITELOG("SplitDataStep", "writeDealLogInfo", "写处理日志信息！", false),
    SPLITDATA_WRITEJED("SplitDataStep", "writeTableToMysql", "写表结构信息到jed！", false),
    SPLITDATA_GETTENANTINFO("SplitDataStep", "getTenantRelInfo", "获取租户信息异常！", true);

    private String serviceName;
    private String methodName;
    private String exceptionDesc;
    private boolean needRetry;

    IntegrationExceptionEnum(String serviceName, String methodName, String exceptionDesc, boolean needRetry) {
        this.serviceName = serviceName;
        this.methodName = methodName;
        this.exceptionDesc = exceptionDesc;
        this.needRetry = needRetry;
    }

    /**
     * 按照serviceCode获得枚举值
     */
    public static IntegrationExceptionEnum valueOf(String serviceName, String methodName) {
        if (StringUtils.isNotBlank(serviceName) && StringUtils.isNotBlank(methodName)) {
            for (IntegrationExceptionEnum integrationExceptionEnum : IntegrationExceptionEnum.values()) {
                if (integrationExceptionEnum.getServiceName().equals(serviceName) && integrationExceptionEnum.getMethodName().equals(methodName)) {
                    return integrationExceptionEnum;
                }
            }
        }
        return null;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public String getExceptionDesc() {
        return exceptionDesc;
    }

    public void setExceptionDesc(String exceptionDesc) {
        this.exceptionDesc = exceptionDesc;
    }

    public boolean isNeedRetry() {
        return needRetry;
    }

    public void setNeedRetry(boolean needRetry) {
        this.needRetry = needRetry;
    }
}

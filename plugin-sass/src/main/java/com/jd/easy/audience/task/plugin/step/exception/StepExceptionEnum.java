package com.jd.easy.audience.task.plugin.step.exception;

import org.apache.commons.lang.StringUtils;
/**
 * @title StepExceptionEnum
 * @description 对step类的异常进行罗列，方便后续进行处理
 * @author cdxiongmei
 * @date 2021/8/10 下午8:20
 * @throws
 */
public enum StepExceptionEnum {
    /**
     * 人群生成异常
     */
    AUDIENCE_GENSQL("AudienceGenerateStep", "generateSql", "异常：人群圈选规则错误！", "Exception: The rule of Audience_Generate is error!", false),
    AUDIENCE_AUDIENCESIZEVALIDATE("AudienceGenerateStep", "audienceSizeValidate", "异常：人群包大小未达到下限！", "Exception: The size of Audience_Package did not reach the lower limit!", false),
    AUDIENCE_SAVETMPPATH("AudienceGenerateStep", "generateTempTable", "失败：数据集或者已有人群读取失败！", "Failure: Datasets or existing Audience_Package failed to read!", true),
    /**
     * 人群上传异常
     */
    AUDIENCE_UPLOAD("AudienceJfsUploadStep", "validate", "错误：人群上传参数校验错误！", "Error: Audience_Upload parameter validation error!", false),
    AUDIENCE_AUDIENCEDATA("AudienceJfsUploadStep", "getTmpData", "失败：人群生成失败！", "Failure: Audience_Generate failed!", true),
    AUDIENCE_VALIDATESIZE("AudienceJfsUploadStep", "validateAudienceSize", "错误：人群包大小超过上限！", "Error: The size of Audience_Package exceeds limit!", false),
    AUDIENCE_SAVETOOSS("AudienceJfsUploadStep", "saveToOss", "失败：人群包保存失败！", "Failure: Audience_Package save ossFile failed!", true),
    /**
     * 人群透视异常
     */
    AUDIENCPROFILE_AUDIENCEDATA("AudienceLabelDatasetProfileStep", "getAudienceData", "异常：获取人群信息异常！", "Exception: Dataset of Audience failed to read!", false),
    AUDIENCPROFILE_LABELDATA("AudienceLabelDatasetProfileStep", "getLabelData", "异常：人群透视标签信息异常！", "Exception: The information of perspective labels is abnormal!", false),
    AUDIENCPROFILE_PROFILE("AudienceLabelDatasetProfileStep", "calcAudienceLabelProfile", "失败：人群透视异常！", "Failure: Audience_Perspective failed!", false),
    AUDIENCPROFILE_MERGEFUNC("AudienceLabelDatasetProfileStep", "mergeFunc", "异常：人群对应标签值解析异常！", "Exception: The value of Audience_Label failed to parse!", false),

    /**
     * RFM人群生成
     */
    RFMAUDIENCE_SAVETOOSS("RfmAudienceGenerateStep", "saveToOss", "失败：人群生成结果保存失败！", "Failure: Audience_Package save ossFile failed!", true),
    RFMAUDIENCE_VALIDLIMIT("RfmAudienceGenerateStep", "validateAudienceSize", "失败：人群上限校验失败！", "Failure: The size of Audience_Package exceeds limit!", false),
    /**
     * 标签数据集
     */
    LABELDATASET_EXESQL("LabelDatasetStep", "buildSql", "异常：标签配置信息存在异常！", "Exception: An exception occurred in the label configuration information!" +
            "\n!", false),
    LABELDATASET_SAVELABELENUM("LabelDatasetStep", "saveLabelEnumData", "失败：保存标签枚举信息失败！", "Failure: Failed to save  enumeration information of labels!", true),

    LABELDATASET_PROFILE("LabelDatasetStep", "calLabelProfile", "异常：标签透视数据计算异常！", "Exception: An exception occurred in the calculation of Label_Perspective!", true),

    LABELDATASET_MERGELABEL("LabelDatasetStep", "explodeTags", "错误：用户标签计算错误！", "Error: An error occurred in the calculation of Label_Perspective!", true),

    /**
     * RFM模型生成
     */
    RFMALALYSIS_SAVETOOSS("RfmAlalysisStep", "saveDataToOss", "失败：保存待分析数据失败！", "Failure: RFM_Datasets failed to save!", true),
    RFMALALYSIS_BUILDDATA("RfmAlalysisStep", "buildSql", "异常：模型数据配置异常！", "Exception: An exception occurred in the configuration information!", false),
    RFMALALYSIS_COMPUTE("RfmAlalysisStep", "computeAlalysis", "错误：模型计算错误！", "Error: RFM_Model build Failed!", false),

    /**
     * jss工具类
     */
    JDCLOUDUTIL_SAVETOOSS("JdCloudOssUtil", "writeDatasetToOss", "保存数据到oss失败！", "Failure: Data failed to save into oss!", true);
    /**
     * 类名
     */
    private String serviceName;
    /**
     * 方法名
     */
    private String methodName;
    /**
     * 异常描述-中文
     */
    private String exceptionDesc;
    /**
     * 异常描述-英文
     */
    private String exceptionEnDesc;
    /**
     * 异常是否需要重试
     */
    private boolean needRetry;
    /**
     * @title StepExceptionEnum
     * @description 构造方法
     * @author cdxiongmei
     * @param: serviceName
     * @param: methodName
     * @param: exceptionDesc
     * @param: exceptionEnDesc
     * @param: needRetry
     * @updateTime 2021/8/10 下午8:19
     * @throws
     */
    StepExceptionEnum(String serviceName, String methodName, String exceptionDesc, String exceptionEnDesc, boolean needRetry) {
        this.serviceName = serviceName;
        this.methodName = methodName;
        this.exceptionDesc = exceptionDesc;
        this.exceptionEnDesc = exceptionEnDesc;
        this.needRetry = needRetry;
    }

    /**
     * 按照serviceCode获得枚举值
     */
    public static StepExceptionEnum valueOf(String serviceName, String methodName) {
        if (StringUtils.isNotBlank(serviceName) && StringUtils.isNotBlank(methodName)) {
            for (StepExceptionEnum stepExceptionEnum : StepExceptionEnum.values()) {
                if (stepExceptionEnum.getServiceName().equals(serviceName) && stepExceptionEnum.getMethodName().equals(methodName)) {
                    return stepExceptionEnum;
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

    public String getExceptionEnDesc() {
        return exceptionEnDesc;
    }

    public void setExceptionEnDesc(String exceptionEnDesc) {
        this.exceptionEnDesc = exceptionEnDesc;
    }
}

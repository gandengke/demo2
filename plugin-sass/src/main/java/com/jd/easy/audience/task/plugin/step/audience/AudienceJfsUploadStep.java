package com.jd.easy.audience.task.plugin.step.audience;

import com.jd.easy.audience.common.audience.AudienceTaskStatus;
import com.jd.easy.audience.common.constant.NumberConstant;
import com.jd.easy.audience.common.constant.StringConstant;
import com.jd.easy.audience.common.exception.JobInterruptedException;
import com.jd.easy.audience.common.oss.JdCloudDataConfig;
import com.jd.easy.audience.common.oss.OssFileTypeEnum;
import com.jd.easy.audience.common.util.JdCloudOssUtil;
import com.jd.easy.audience.task.commonbean.bean.AudienceJfsUploadBean;
import com.jd.easy.audience.task.driven.step.StepCommon;
import com.jd.easy.audience.task.driven.step.StepCommonBean;
import com.jd.easy.audience.task.plugin.property.ConfigProperties;
import com.jd.easy.audience.task.plugin.run.spark.SparkLauncherUtil;
import com.jd.easy.audience.task.plugin.util.SparkUdfUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * @author cdxiongmei
 * @title
 * @description 将人群包的pin数据Dataset保存到JFS中。 返回值为null
 * @date 2021/8/10 上午10:47
 */
public class AudienceJfsUploadStep extends StepCommon<Map<String, Object>> {
    /**
     * log工具
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(AudienceJfsUploadStep.class);

    @Override
    public void validate() {
        super.validate();
        AudienceJfsUploadBean bean;
        if (getStepBean() instanceof AudienceJfsUploadBean) {
            bean = (AudienceJfsUploadBean) getStepBean();
        } else {
            throw new JobInterruptedException("bean 参数异常", "bean parameter occurs errors;");
        }
        if (StringUtils.isBlank(bean.getStepName())) {
            throw new RuntimeException("stepName is null in AudienceJfsUploadStep");
        }
        if (StringUtils.isBlank(bean.getAudienceDatasetStepName())) {
            throw new RuntimeException("audienceDatasetStepName is null in AudienceJfsUploadStep");
        }
        if (StringUtils.isBlank(bean.getJfsFilePath())) {
            throw new RuntimeException("jfsFilePath is null in AudienceJfsUploadStep");
        }
        if (StringUtils.isBlank(bean.getFileName())) {
            throw new RuntimeException("fileName is null in AudienceJfsUploadStep");
        }
        if (null == bean.getIfLimit()) {
            bean.setIfLimit(false);
        }
    }

    /**
     * @title getTmpData
     * @description 获取上游任务生成的分布式数据集
     * @author cdxiongmei
     * @param: dependencies
     * @updateTime 2021/8/10 上午10:47
     * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     */
    public Dataset<Row> getTmpData(Map<String, StepCommonBean> dependencies, AudienceJfsUploadBean bean) {
        Dataset<Row> tmpData = null;
        final StepCommonBean generateBean = dependencies.get(bean.getAudienceDatasetStepName());
        if (generateBean != null) {
            tmpData = (Dataset<Row>) ((Map<String, Object>) generateBean.getData()).get("tmpData");
            tmpData.cache();
            LOGGER.info("audience dataset tmpData cnt =" + tmpData.count());
        }
        if (null == tmpData) {
            LOGGER.error("Failed to save dataset to JFS because dataset from previous step: {} is null.",
                    generateBean.getStepName());
            throw new JobInterruptedException("人群生成结果保存失败");
        }
        return tmpData;
    }

    /**
     * @throws
     * @title saveToOss
     * @description 将人群包保存到oss
     * @author cdxiongmei
     * @param: sparkSession
     * @param: dataAudience
     * @param: bucketName
     * @param: ossPath
     * @param: fileName
     * @updateTime 2021/8/10 上午10:48
     */
    public void saveToOss(SparkSession sparkSession, Dataset<Row> dataAudience, String ossPath, AudienceJfsUploadBean bean) {
        //1. jfs协议文件路径
        String filePath = bean.getJfsBucket() + File.separator + ossPath + File.separator + bean.getFileName();
        //2. 将人群写入oss
        bean.getOutputMap().put(ConfigProperties.AUDIENCE_STATUS_KEY, AudienceTaskStatus.WRITE_AUDIENCE_JFS.name());
        LOGGER.info("write csv to oss path:filePath=" + filePath);
        //3. oss文件下载路径
        JdCloudDataConfig config = new JdCloudDataConfig(bean.getJfsEndPoint(), bean.getJfsAccessKey(), bean.getJfsSecretKey());
        config.setOptions(new HashMap<String, String>() {
            {
                put("header", "true");
            }
        });
        config.setClientType(ConfigProperties.getClientType());
        config.setFileType(OssFileTypeEnum.csv);
        config.setData(dataAudience);
        config.setObjectKey(filePath);
        bean.setAudiencePackageUrl(JdCloudOssUtil.writeDatasetToOss(sparkSession, config));
    }

    @Override
    public Map<String, Object> run(Map<String, StepCommonBean> dependencies) {

        //1. 初始化
        AudienceJfsUploadBean bean = null;
        if (getStepBean() instanceof AudienceJfsUploadBean) {
            bean = (AudienceJfsUploadBean) getStepBean();
        } else {
            throw new JobInterruptedException("bean 参数异常", "bean parameter occurs errors;");
        }
        String filePath = SparkUdfUtil.dealPath(bean.getJfsFilePath());
        Map<String, Object> outputMap = new HashMap<>(NumberConstant.INT_20);
        bean.setOutputMap(outputMap);
        SparkSession sparkSession = SparkLauncherUtil.buildSparkSession(bean.getStepName());

        //2. 人群的临时数据获取
        Dataset<Row> audienceData = getTmpData(dependencies, bean);
        //3. 人群oss路径处理

        LOGGER.info("Old jfsFilePath: {} will be deleted", filePath);
        //4. 人群状态
        outputMap.put(ConfigProperties.AUDIENCE_STATUS_KEY, AudienceTaskStatus.CLEAN_OLD_AUDIENCE_JFS.name());
        Dataset<Row> dataAudience = validateAudienceSize(audienceData, bean);
        //5. save to oss
        saveToOss(sparkSession, dataAudience, filePath, bean);
        outputMap.put("audiencePackageUrl", bean.getAudiencePackageUrl());
        outputMap.put("objectKey", bean.getJfsFilePath() + File.separator + bean.getFileName());
        outputMap.put("audienceSize", dataAudience.count());
        return outputMap;
    }

    /**
     * @throws
     * @title validateAudienceSize
     * @description 校验人群包是否超过上限
     * @author cdxiongmei
     * @param: dataFrame
     * @updateTime 2021/8/10 上午10:48
     * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     */
    public Dataset<Row> validateAudienceSize(Dataset<Row> dataFrame, AudienceJfsUploadBean bean) {
        long audienceCnt = dataFrame.count();
        //1. 人群截取且人群超过配置的上限
        boolean overLimit = (null == bean.getAudienceType() || audienceCnt > Long.parseLong(bean.getAudienceLimitNum()));
        if (!bean.getIfLimit() && overLimit) {
            String str = String.format("人群包超过最大值 %s,请重新定义人群包", bean.getAudienceLimitNum());
            bean.getOutputMap().put(ConfigProperties.AUDIENCE_STATUS_KEY, AudienceTaskStatus.ERROR.name() + StringConstant.COLON + str);
            throw new JobInterruptedException(str, "The size of audience package is over than " + bean.getAudienceLimitNum());
        }
        //2. 人群可以使用limit截取
        if (bean.getIfLimit() && overLimit) {
            dataFrame = dataFrame.limit(Math.toIntExact(Long.parseLong(bean.getAudienceLimitNum())));
        }
        return dataFrame;
    }

    @Override
    public String toString() {
        AudienceJfsUploadBean bean = null;
        if (getStepBean() instanceof AudienceJfsUploadBean) {
            bean = (AudienceJfsUploadBean) getStepBean();
        } else {
            throw new JobInterruptedException("bean 参数异常", "bean parameter occurs errors;");
        }
        return "AudienceJfsUploadStep{" +
                "audienceDatasetStepName='" + bean.getAudienceDatasetStepName() + '\'' +
                ", jfsFilePath='" + bean.getJfsFilePath() + '\'' +
                ", fileName='" + bean.getFileName() + '\'' +
                ", jfsBucket='" + bean.getJfsBucket() + '\'' +
                ", jfsAccessKey='" + bean.getJfsAccessKey() + '\'' +
                ", jfsSecretKey='" + bean.getJfsSecretKey() + '\'' +
                ", jfsEndPoint='" + bean.getJfsEndPoint() + '\'' +
                ", audienceLimitNum='" + bean.getAudienceLimitNum() + '\'' +
                ", audiencePackageUrl='" + bean.getAudiencePackageUrl() + '\'' +
                ", audienceJfsObjectKey='" + bean.getAudienceJfsObjectKey() + '\'' +
                ", audienceJfsFileMd5='" + bean.getAudienceJfsFileMd5() + '\'' +
                ", audienceType='" + bean.getAudienceType() + '\'' +
                ", ifLimit=" + bean.getIfLimit() +
                '}';
    }
}

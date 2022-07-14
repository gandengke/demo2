package com.jd.easy.audience.task.plugin.step.audience;

import com.jd.easy.audience.common.audience.AudienceTaskStatus;
import com.jd.easy.audience.common.constant.NumberConstant;
import com.jd.easy.audience.common.exception.JobInterruptedException;
import com.jd.easy.audience.common.oss.JdCloudDataConfig;
import com.jd.easy.audience.common.oss.OssFileTypeEnum;
import com.jd.easy.audience.common.util.JdCloudOssUtil;
import com.jd.easy.audience.task.commonbean.bean.AudienceSplitBean;
import com.jd.easy.audience.task.commonbean.bean.CommonSubPackageBean;
import com.jd.easy.audience.task.driven.step.StepCommon;
import com.jd.easy.audience.task.driven.step.StepCommonBean;
import com.jd.easy.audience.task.plugin.property.ConfigProperties;
import com.jd.easy.audience.task.plugin.run.spark.SparkLauncherUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author cdxiongmei
 * @title
 * @description 将人群包的按照不同比例进行拆分
 * @date 2021/8/17 上午10:47
 * @throws
 */
public class AudienceSplitStep extends StepCommon<Map<String, Object>> {
    /**
     * log工具
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(AudienceSplitStep.class);

    @Override
    public void validate() {
        super.validate();
        AudienceSplitBean bean = null;
        if (getStepBean() instanceof AudienceSplitBean) {
            bean = (AudienceSplitBean) getStepBean();
        } else {
            throw new JobInterruptedException("bean 参数异常", "bean parameter occurs errors;");
        }
        if (StringUtils.isBlank(bean.getStepName())) {
            throw new RuntimeException("stepName is null in AudienceJfsUploadStep");
        }
        if (null == bean.getSubPackages() || bean.getSubPackages().isEmpty()) {
            throw new RuntimeException("subpackage config is null in AudienceSplitStep");
        }
        if (StringUtils.isBlank(bean.getAudienceDataFile())) {
            throw new RuntimeException("audience audienceDataFile is null in AudienceSplitStep");
        }
        Double ratioPlus = 0.0D;
        for (CommonSubPackageBean subBean : bean.getSubPackages()) {
            if (subBean.getRatio() <= BigDecimal.ZERO.doubleValue() || subBean.getRatio() > BigDecimal.ONE.doubleValue()) {
                throw new RuntimeException("subpackage config is wrong in AudienceSplitStep");
            }
            ratioPlus = ratioPlus + subBean.getRatio();

        }
        if (ratioPlus > BigDecimal.ONE.doubleValue()) {
            throw new RuntimeException("subpackage config is wrong in AudienceSplitStep");
        }
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
    public void saveToOss(SparkSession sparkSession, Dataset<Row> dataAudience, String ossFileKey, AudienceSplitBean bean) {
        //1. jfs协议文件路径
        String filePath = bean.getBucket() + File.separator + ossFileKey;
        //2. 将人群写入oss
        bean.getOutputMap().put(ConfigProperties.AUDIENCE_STATUS_KEY, AudienceTaskStatus.WRITE_AUDIENCE_JFS.name());
        LOGGER.info("write csv to oss path:filePath=" + filePath);
        //3. oss文件下载路径
        JdCloudDataConfig config = new JdCloudDataConfig(bean.getEndPoint(), bean.getAccessKey(), bean.getSecretKey());
        config.setOptions(new HashMap<String, String>() {
            {
                put("header", "true");
            }
        });
        config.setClientType(ConfigProperties.getClientType());
        config.setFileType(OssFileTypeEnum.csv);
        config.setData(dataAudience);
        config.setObjectKey(filePath);
        JdCloudOssUtil.writeDatasetToOss(sparkSession, config);
    }

    @Override
    public Map<String, Object> run(Map<String, StepCommonBean> dependencies) {

        //1. 初始化
        AudienceSplitBean bean = null;
        if (getStepBean() instanceof AudienceSplitBean) {
            bean = (AudienceSplitBean) getStepBean();
        } else {
            throw new JobInterruptedException("bean 参数异常", "bean parameter occurs errors;");
        }
        Map<String, Object> outputMap = new HashMap<>(NumberConstant.INT_20);
        bean.setOutputMap(outputMap);
        SparkSession sparkSession = SparkLauncherUtil.buildSparkSession(bean.getStepName());

        //2. 人群的临时数据获取
        Dataset<Row> audienceDataset = getAudienceData(sparkSession, bean).selectExpr("user_id", "ROW_NUMBER() over(ORDER BY user_id) as rank");
        audienceDataset.cache();
        Long audienceCnt = audienceDataset.count();
        LOGGER.info("audience size ={}", audienceCnt);
        Map<Long, Long> packagesSize = new HashMap<>(NumberConstant.INT_20);
        Long startPos = NumberConstant.LONG_0;

        //3. 人群oss路径处理
        for (CommonSubPackageBean subBean : bean.getSubPackages()) {
            if (subBean.getRatio() <= BigDecimal.ZERO.doubleValue() || subBean.getRatio() > BigDecimal.ONE.doubleValue()) {
                LOGGER.info("子包配置异常,{}", subBean.toString());
                continue;
            }
            outputMap.put(ConfigProperties.AUDIENCE_STATUS_KEY, AudienceTaskStatus.CLEAN_OLD_AUDIENCE_JFS.name());
            //按照比例进行拆包
            Long subCount = Long.valueOf(Double.valueOf(Math.ceil(audienceCnt * subBean.getRatio())).longValue());
            if (subCount == BigDecimal.ZERO.doubleValue()) {
                continue;
            }
            Long endPos = startPos + subCount;
            Dataset<Row> subData = audienceDataset.filter("rank >" + startPos + " and rank <=" + endPos).drop("rank");
            startPos = endPos;
            String ossKey = JdCloudOssUtil.dealPath(subBean.getJfsFilePath()) + File.separator + subBean.getFileName();
            saveToOss(sparkSession, subData, ossKey, bean);
            packagesSize.put(subBean.getAudienceId(), subData.count());
        }
        outputMap.put("packageSize", packagesSize);
        outputMap.put("status", NumberConstant.INT_1);
        return outputMap;
    }

    /**
     * @throws
     * @title getAudienceData
     * @description 从oss读取人群包明细数据
     * @author cdxiongmei
     * @param: spark
     * @updateTime 2021/8/10 上午10:52
     * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     */
    public Dataset<Row> getAudienceData(SparkSession spark, AudienceSplitBean bean) {
        //1. jsf协议的oss路径
        String jfsPath = File.separator + bean.getBucket() + File.separator + URI.create(bean.getAudienceDataFile()).getPath();
        LOGGER.info("从oss获取人群包信息jfsPath={}", jfsPath);
        //2. 读取oss数据
        JdCloudDataConfig readConfig = new JdCloudDataConfig(bean.getEndPoint(), bean.getAccessKey(), bean.getSecretKey());
        readConfig.setOptions(new HashMap<String, String>() {
            {
                put("header", "true");
            }
        });
        readConfig.setClientType(ConfigProperties.getClientType());
        readConfig.setFileType(OssFileTypeEnum.csv);
        readConfig.setObjectKey(jfsPath);
        Dataset<Row> dataset = JdCloudOssUtil.readDatasetFromOss(spark, readConfig);

        //3. 校验人群包数据
        if (null == dataset || dataset.count() == BigInteger.ZERO.intValue()) {
            String str = "没有查到该人群生成结果数据,人群dataKey=" + bean.getAudienceDataFile();
            throw new JobInterruptedException(str, "audience is not exsit!audienceKey=" + bean.getAudienceDataFile());
        }
        return dataset;
    }

    @Override
    public String toString() {
        AudienceSplitBean bean = null;
        if (getStepBean() instanceof AudienceSplitBean) {
            bean = (AudienceSplitBean) getStepBean();
        } else {
            throw new JobInterruptedException("bean 参数异常", "bean parameter occurs errors;");
        }
        return "AudienceSplitBean{" +
                "stepName='" + bean.getStepName() + '\'' +
                ", audienceDataFile='" + bean.getAudienceDataFile() + '\'' +
                ", jfsBucket='" + bean.getBucket() + '\'' +
                ", jfsAccessKey='" + bean.getAccessKey() + '\'' +
                ", jfsSecretKey='" + bean.getSecretKey() + '\'' +
                ", jfsEndPoint='" + bean.getEndPoint() + '\'' +
                ", subPackages=" + bean.getSubPackages().toString() +
                '}';
    }
}

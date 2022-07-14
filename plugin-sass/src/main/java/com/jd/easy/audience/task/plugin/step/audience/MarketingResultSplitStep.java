package com.jd.easy.audience.task.plugin.step.audience;

import com.jd.easy.audience.common.audience.AudienceTaskStatus;
import com.jd.easy.audience.common.constant.MarketingResultSplitEnum;
import com.jd.easy.audience.common.constant.NumberConstant;
import com.jd.easy.audience.common.constant.StringConstant;
import com.jd.easy.audience.common.exception.JobInterruptedException;
import com.jd.easy.audience.common.oss.JdCloudDataConfig;
import com.jd.easy.audience.common.oss.OssFileTypeEnum;
import com.jd.easy.audience.common.util.JdCloudOssUtil;
import com.jd.easy.audience.task.commonbean.bean.MarketingResultSplitBean;
import com.jd.easy.audience.task.commonbean.bean.MarketingResultSplitSubBean;
import com.jd.easy.audience.task.driven.step.StepCommon;
import com.jd.easy.audience.task.driven.step.StepCommonBean;
import com.jd.easy.audience.task.plugin.property.ConfigProperties;
import com.jd.easy.audience.task.plugin.run.spark.SparkLauncherUtil;
import com.typesafe.config.Config;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigInteger;
import java.net.URI;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * @author cdxiongmei
 * @title
 * @description 将营销回执明细数据按照状态拆分
 * @date 2021/8/17 上午10:47
 * @throws
 */
public class MarketingResultSplitStep extends StepCommon<Map<String, Object>> {
    /**
     * log工具
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(MarketingResultSplitStep.class);

    @Override
    public void validate() {
        super.validate();
        MarketingResultSplitBean bean = null;
        if (getStepBean() instanceof MarketingResultSplitBean) {
            bean = (MarketingResultSplitBean) getStepBean();
        } else {
            throw new JobInterruptedException("bean 参数异常", "bean parameter occurs errors;");
        }
        if (StringUtils.isBlank(bean.getStepName())) {
            throw new RuntimeException("stepName is null in MarketingResultSplitStep");
        }
        if (null == bean.getSubPackages() || bean.getSubPackages().isEmpty()) {
            throw new RuntimeException("subpackage config is null in MarketingResultSplitStep");
        }
        if (StringUtils.isBlank(bean.getAudienceDataFile())) {
            throw new RuntimeException("audience packageurl is null in MarketingResultSplitStep");
        }
        if (StringUtils.isBlank(bean.getBusinessId())) {
            throw new RuntimeException("audience businessId is null in MarketingResultSplitStep");
        }
        if (StringUtils.isBlank(bean.getSource())) {
            throw new RuntimeException("audience source is null in MarketingResultSplitStep");
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
    public void saveToOss(SparkSession sparkSession, Dataset<Row> dataAudience, String ossFileKey, MarketingResultSplitBean bean) {
        //1. jfs协议文件路径
        String filePath = bean.getBucket() + File.separator + ossFileKey;
        //2. 将人群写入oss
        bean.getOutputMap().put(ConfigProperties.AUDIENCE_STATUS_KEY, AudienceTaskStatus.WRITE_AUDIENCE_JFS.name());
        LOGGER.info("write csv to oss path:filePath=" + filePath);
        //3. oss文件下载路径
        JdCloudDataConfig config = new JdCloudDataConfig(bean.getEndPoint(), bean.getAccessKey(), bean.getSecretKey());
        config.setClientType(ConfigProperties.getClientType());
        config.setFileType(OssFileTypeEnum.csv);
        config.setData(dataAudience);
        config.setObjectKey(filePath);
        config.setOptions(new HashMap<String, String>() {
            {
                put("header", "true");
            }
        });
        JdCloudOssUtil.writeDatasetToOss(sparkSession, config);
    }

    @Override
    public Map<String, Object> run(Map<String, StepCommonBean> dependencies) {

        //1. 初始化
        MarketingResultSplitBean bean = null;
        if (getStepBean() instanceof MarketingResultSplitBean) {
            bean = (MarketingResultSplitBean) getStepBean();
        } else {
            throw new JobInterruptedException("bean 参数异常", "bean parameter occurs errors;");
        }
        Map<String, Object> outputMap = new HashMap<>(NumberConstant.INT_20);
        bean.setOutputMap(outputMap);
        SparkSession sparkSession = SparkLauncherUtil.buildSparkSession(bean.getStepName());
        //2. 人群oss路径处理
        Dataset<Row> marketDetail = getMarketingData(sparkSession, bean);
        //3. 校验人群包数据
        if (null == marketDetail || marketDetail.count() == BigInteger.ZERO.intValue()) {
            String str = "没有查到营销明细数据,source=" + bean.getSource() + ", businessId=" + bean.getBusinessId();
            LOGGER.error(str);
            throw new JobInterruptedException(str, "audience is not exsit!audienceKey=" + bean.getAudienceDataFile());
        }
        marketDetail.cache();
        Map<Long, Long> packagesSize = new HashMap<>(NumberConstant.INT_20);
        for (MarketingResultSplitSubBean subBean : bean.getSubPackages()) {
            outputMap.put(ConfigProperties.AUDIENCE_STATUS_KEY, AudienceTaskStatus.CLEAN_OLD_AUDIENCE_JFS.name());
            //按照比例进行拆包
            String[] statusList = subBean.getFilterRules().split(StringConstant.COMMA);
            Dataset<Row> subData = marketDetail.filter("status in (\"" + StringUtils.join(statusList, "\",\"") + "\")").select("user_id");
            //如果是差集
            if (subBean.getFilterType().equals(MarketingResultSplitEnum.diff)) {
                Dataset<Row> audienceData = getAudienceData(sparkSession, bean);
                subData = audienceData.except(subData);
            }
            LOGGER.info("子人群包结果打印：" + subBean.getAudienceId());
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
    public Dataset<Row> getAudienceData(SparkSession spark, MarketingResultSplitBean bean) {
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

    /**
     * 从营销回执明细中获取数据
     *
     * @param spark
     * @param bean
     * @return
     */
    public Dataset<Row> getMarketingData(SparkSession spark, MarketingResultSplitBean bean) {
        //1. jsf协议的oss路径
        LOGGER.info(this.getClass().getSimpleName());
        Config templateConfig = ConfigProperties.getsqlTemplate(this.getClass().getSimpleName(), new Exception().getStackTrace()[0].getMethodName());
        String marketTb = ConfigProperties.MARKETING_DETAIL;
//        String marketSql = "SELECT user_id,status FROM  " +
//                marketTb + " WHERE source =  '" +
//                bean.getSource() + "' and business_id='" +
//                bean.getBusinessId() + "' group by user_id, status\n";
        String marketSql = MessageFormat.format(templateConfig.getString("smsResultSearch"), marketTb, bean.getSource(), bean.getBusinessId());
        LOGGER.info("从营销明细表中获取信息{}", marketSql);
        Dataset<Row> ds = spark.sql(marketSql);
        ds.show(NumberConstant.INT_10);
        return ds;
    }

    @Override
    public String toString() {
        MarketingResultSplitBean bean = null;
        if (getStepBean() instanceof MarketingResultSplitBean) {
            bean = (MarketingResultSplitBean) getStepBean();
        } else {
            throw new JobInterruptedException("bean 参数异常", "bean parameter occurs errors;");
        }
        return "MarketingResultSplitBean{" +
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

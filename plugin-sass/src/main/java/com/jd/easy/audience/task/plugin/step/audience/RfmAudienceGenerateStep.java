package com.jd.easy.audience.task.plugin.step.audience;

import com.jd.easy.audience.common.audience.AudienceTaskStatus;
import com.jd.easy.audience.common.constant.NumberConstant;
import com.jd.easy.audience.common.constant.StringConstant;
import com.jd.easy.audience.common.exception.JobInterruptedException;
import com.jd.easy.audience.common.oss.JdCloudDataConfig;
import com.jd.easy.audience.common.oss.OssFileTypeEnum;
import com.jd.easy.audience.common.util.JdCloudOssUtil;
import com.jd.easy.audience.task.commonbean.bean.RfmAudienceGenerateBean;
import com.jd.easy.audience.task.driven.step.StepCommon;
import com.jd.easy.audience.task.driven.step.StepCommonBean;
import com.jd.easy.audience.task.generator.service.GenerateSQLService;
import com.jd.easy.audience.task.generator.service.impl.GenerateSQLServiceImpl;
import com.jd.easy.audience.task.generator.util.GeneratorUtil;
import com.jd.easy.audience.task.plugin.property.ConfigProperties;
import com.jd.easy.audience.task.plugin.run.spark.SparkLauncherUtil;
import com.jd.easy.audience.task.plugin.util.SparkUdfUtil;
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
 * @title RfmAudienceGenerateStep
 * @description 根据配置规则生成人群
 * @date 2021/8/10 上午11:22
 * @throws
 */
public class RfmAudienceGenerateStep extends StepCommon<Map<String, Object>> {
    /**
     * log工具
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(RfmAudienceGenerateStep.class);

    @Override
    public Map<String, Object> run(Map<String, StepCommonBean> dependencies) throws Exception {
        LOGGER.info("rfmAudienceGenerateStep Start Run ");
        //1.初始化启动sparksession
        RfmAudienceGenerateBean bean = null;
        if (getStepBean() instanceof RfmAudienceGenerateBean) {
            bean = (RfmAudienceGenerateBean) getStepBean();
        } else {
            throw new JobInterruptedException("bean 参数异常", "bean parameter occurs errors;");
        }
        SparkSession spark = SparkLauncherUtil.buildSparkSession(bean.getStepName());
        Map<String, Object> outputMap = new HashMap<>(NumberConstant.INT_10);
        bean.setOutputMap(outputMap);
        outputMap.put(ConfigProperties.AUDIENCE_STATUS_KEY, AudienceTaskStatus.EXECUTE_QUERY.name());
        //2. 引入sql生成引擎
        GenerateSQLService generateSqlService = new GenerateSQLServiceImpl();
        String sql = generateSqlService.generateSparkSQL(bean.getSparkSql());
        LOGGER.info("sparkSql: {}", sql);
        //3. 加载oss人群parquet明细数据
        String userObjectKey = JdCloudOssUtil.getRfmUserDetDataKey(bean.getInputFilePath());

        JdCloudDataConfig readConfig = new JdCloudDataConfig(bean.getJfsEndPoint(), bean.getJfsAccessKey(), bean.getJfsSecretKey());
        readConfig.setOptions(new HashMap<String, String>() {
            {
                put("header", "true");
            }
        });
        readConfig.setClientType(ConfigProperties.getClientType());
        readConfig.setFileType(OssFileTypeEnum.parquet);
        readConfig.setObjectKey(bean.getJfsBucket() + File.separator + userObjectKey);
        Dataset<Row> userDet = JdCloudOssUtil.readDatasetFromOss(spark, readConfig);
        //获取临时表
        Map<String, Object> beansMap = GeneratorUtil.getrfmBeans();
        String tmpTable = beansMap.keySet().stream().findFirst().get();
        LOGGER.info("ready write temp view :{}", tmpTable);
        userDet.createOrReplaceTempView(tmpTable);
        Dataset<Row> audiencePackage = spark.sql(sql);
        //4. 人群包大小校验
        Dataset<Row> audiencePackageLimit = validateAudienceSize(audiencePackage, bean);

        //5. 写入oss下
        String packageUrl = saveToOss(spark, audiencePackageLimit, bean);

        //6. 数据返回
        outputMap.put("audienceSize", audiencePackageLimit.count());
        outputMap.put("audiencePackageUrl", packageUrl);
        outputMap.put("objectKey", bean.getJfsFilePath() + File.separator + bean.getFileName());

        //7. 现场清理
        return outputMap;
    }

    /**
     * 保存数据到oss
     *
     * @param sparkSession
     * @param audiencePackage
     * @return
     */
    public String saveToOss(SparkSession sparkSession, Dataset<Row> audiencePackage, RfmAudienceGenerateBean bean) {
        //1. 处理链接
        String filePath = SparkUdfUtil.dealPath(bean.getJfsFilePath());
        String csvPathName = bean.getJfsBucket() + File.separator + filePath + File.separator + bean.getFileName();
        LOGGER.info("save to  csv, jfsKey={}", csvPathName);
        //2. 将人群写入oss
        bean.getOutputMap().put(ConfigProperties.AUDIENCE_STATUS_KEY, AudienceTaskStatus.WRITE_AUDIENCE_JFS.name());
        LOGGER.info("write csv to hdfs path:filePath={}", csvPathName);
        JdCloudDataConfig config = new JdCloudDataConfig(bean.getJfsEndPoint(), bean.getJfsAccessKey(), bean.getJfsSecretKey());
        config.setOptions(new HashMap<String, String>() {
            {
                put("header", "true");
            }
        });
        config.setClientType(ConfigProperties.getClientType());
        config.setFileType(OssFileTypeEnum.csv);
        config.setData(audiencePackage);
        config.setObjectKey(csvPathName);
        String packageUrl = JdCloudOssUtil.writeDatasetToOss(sparkSession, config);
        //3. 返回下载链接
        return packageUrl;
    }

    /**
     * 人群上限校验
     *
     * @param dataFrame
     * @param bean
     * @return
     */
    public Dataset<Row> validateAudienceSize(Dataset<Row> dataFrame, RfmAudienceGenerateBean bean) {
        long audienceCnt = dataFrame.count();
        if (audienceCnt < bean.getLimitSize()) {
            throw new JobInterruptedException("人群包大小小于" + bean.getLimitSize(), "Audience package size less than " + bean.getLimitSize());
        }
        //1. 不能使用limit截取
        boolean overLimit = (audienceCnt > Long.parseLong(bean.getAudienceLimitNum()));
        if (!bean.getIfLimit() && overLimit) {
            String str = String.format("人群包超过最大值 %s,请重新定义人群包", bean.getAudienceLimitNum());
            bean.getOutputMap().put(ConfigProperties.AUDIENCE_STATUS_KEY, AudienceTaskStatus.ERROR.name() + StringConstant.COLON + str);
            throw new JobInterruptedException(str);
        }
        //2. 可以使用limit截取
        if (bean.getIfLimit() && overLimit) {
            dataFrame = dataFrame.limit(Math.toIntExact(Long.parseLong(bean.getAudienceLimitNum())));
        }
        return dataFrame;
    }
}
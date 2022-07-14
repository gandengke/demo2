package com.jd.easy.audience.task.plugin.step.audience;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.jd.easy.audience.common.constant.LogicalOperatorEnum;
import com.jd.easy.audience.common.exception.JobInterruptedException;
import com.jd.easy.audience.common.oss.JdCloudDataConfig;
import com.jd.easy.audience.common.oss.OssFileTypeEnum;
import com.jd.easy.audience.common.util.JdCloudOssUtil;
import com.jd.easy.audience.task.commonbean.bean.AudienceProfileBean;
import com.jd.easy.audience.task.driven.step.StepCommon;
import com.jd.easy.audience.task.driven.step.StepCommonBean;
import com.jd.easy.audience.task.plugin.property.ConfigProperties;
import com.jd.easy.audience.task.plugin.run.spark.SparkLauncherUtil;
import com.jd.easy.audience.task.plugin.util.es.EsUtil;
import com.jd.easy.audience.task.plugin.util.es.bean.EsCluster;
import com.jd.easy.audience.task.plugin.util.es.sqltool.ComparisonOperatorsEnum;
import com.jd.easy.audience.task.plugin.util.es.sqltool.SqlCondition;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigInteger;
import java.net.URI;
import java.util.*;

/**
 * @author cdxiongmei
 * @title AudienceLabelDatasetProfileStep
 * @description 人群透视分析
 * @date 2021/3/21 上午10:51
 * @throws
 */
@Deprecated
public class AudienceLabelDatasetProfileStep extends StepCommon<Void> {
    /**
     * log工具
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(AudienceLabelDatasetProfileStep.class);
    /**
     * 枚举值总数的限制
     */
    private static final Integer PROFILE_COUNT_MAX = 50;

    /**
     * Run a step from the dependency steps.
     *
     * @param dependencies
     * @return T result set.
     * @throws Exception
     */
    @Override
    public Void run(Map<String, StepCommonBean> dependencies) throws Exception {
        //1. sparksession初始化-包含es的配置
        AudienceProfileBean bean = null;
        if (getStepBean() instanceof AudienceProfileBean) {
            bean = (AudienceProfileBean) getStepBean();
        } else {
            throw new JobInterruptedException("bean 参数异常", "bean parameter occurs errors;");
        }
        deleteEsData(bean);
        SparkSession sparkSession = SparkLauncherUtil.buildSparkSessionWithESSettingsMultiNodes(bean.getEsNode(), bean.getEsPort(), bean.getEsUser(), bean.getEsPass(), bean.getStepName());
        //2. 加载人群数据
        Dataset<Row> audienceDataset = getAudienceData(sparkSession, bean);
        audienceDataset.cache();
        //3. 加载标签集明细数据,label数据和人群数据join, 聚合并保存记录
        JavaRDD<Row> allLabelData = getLabelData(sparkSession, audienceDataset, bean);
        //4. 准备写入es
        Map<String, String> map = new HashMap<>(16);
        map.put("es.mapping.id", "id");
        JavaEsSparkSQL.saveToEs(calcAudienceLabelProfile(sparkSession, allLabelData, Long.valueOf(audienceDataset.count())), bean.getEsIndex(), map);
        return null;
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
    public Dataset<Row> getAudienceData(SparkSession spark, AudienceProfileBean bean) {
        //1. jsf协议的oss路径
        String jfsPath = File.separator + bean.getBucket() + File.separator + URI.create(bean.getAudienceDataFile()).getPath();
        LOGGER.info("从oss获取人群包信息jfsPath={}", jfsPath);
        //2. 读取oss数据
        JdCloudDataConfig readConfig = new JdCloudDataConfig(bean.getEndpoint(), bean.getAccessKey(), bean.getSecretKey());
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
            String str = "没有查到该人群生成结果数据,人群id=" + bean.getAudienceId();
            throw new JobInterruptedException(str, "audience is not exsit!audienceId=" + bean.getAudienceId());
        }
        LOGGER.info("audience size ={}", dataset.count());
        return dataset;
    }

    /**
     * @throws
     * @title getLabelData
     * @description 获取标签数据集标签明细数据
     * @author cdxiongmei
     * @param: spark
     * @param: audienceData
     * @updateTime 2021/8/10 上午10:55
     * @return: org.apache.spark.api.java.JavaRDD<org.apache.spark.sql.Row>
     */
    public JavaRDD<Row> getLabelData(SparkSession spark, Dataset<Row> audienceData, AudienceProfileBean bean) {
        JavaRDD<Row> allLabelData = null;
        for (Map.Entry<Long, String> entry : bean.getLabelFilePath().entrySet()) {
            //1. 获取标签数据集明细信息
            String jfsObjectKey = bean.getBucket() + File.separator + entry.getValue();
            JdCloudDataConfig readConfig = new JdCloudDataConfig(bean.getEndpoint(), bean.getAccessKey(), bean.getSecretKey());
            readConfig.setOptions(new HashMap<String, String>() {
                {
                    put("header", "true");
                }
            });
            readConfig.setClientType(ConfigProperties.getClientType());
            readConfig.setFileType(OssFileTypeEnum.csv);
            readConfig.setObjectKey(jfsObjectKey);
            Dataset<Row> labelDataset = JdCloudOssUtil.readDatasetFromOss(spark, readConfig);
            //2. 与人群join
            Dataset<Row> audienceLabelData = labelDataset.join(audienceData, "user_id")
                    .withColumn("audienceId", functions.lit(bean.getAudienceId()))
                    .withColumn("datasetId", functions.lit(entry.getKey()));
            LOGGER.info("join with audience result count={}", audienceLabelData.count());
            //3. 解析标签的tag信息（将多值的字段进行展开）
            JavaRDD<Row> rowDataset = audienceLabelData.mapPartitions((MapPartitionsFunction<Row, Row>) iterator ->
                    mergeFunc(iterator, bean), Encoders.bean(Row.class)).toJavaRDD();

            //4. 结果数据集合并
            if (null == allLabelData) {
                allLabelData = rowDataset;
            } else {
                allLabelData = allLabelData.union(rowDataset);
            }
        }
        //5. 结果数据集校验
        if (null == allLabelData || allLabelData.count() == BigInteger.ZERO.intValue()) {
            String str = "透视结果的人群数为0,人群id=" + bean.getAudienceId();
            LOGGER.info(str);
            throw new JobInterruptedException(str);

        }
        return allLabelData;
    }

    /**
     * 计算人群标签
     */
    @JsonIgnore
    public Dataset<Row> calcAudienceLabelProfile(SparkSession spark, JavaRDD<Row> audienceDataset, Long audienceSize) {

        //1. structfield信息
        List<StructField> fieldList = new ArrayList<>();
        fieldList.add(DataTypes.createStructField("audienceId", DataTypes.LongType, false));
        fieldList.add(DataTypes.createStructField("datasetId", DataTypes.LongType, false));
        fieldList.add(DataTypes.createStructField("labelId", DataTypes.LongType, false));
        fieldList.add(DataTypes.createStructField("labelName", DataTypes.StringType, false));
        fieldList.add(DataTypes.createStructField("labelValue", DataTypes.StringType, true));
        fieldList.add(DataTypes.createStructField("userId", DataTypes.StringType, false));
        StructType struct = DataTypes.createStructType(fieldList);
        spark.createDataFrame(audienceDataset, struct).createOrReplaceTempView("audience_profile_det");

        //2. 聚合计算透视分析结果
        Dataset<Row> result = spark.sql("select\n" +
                "\taudienceId,\n" +
                "\tdatasetId,\n" +
                "\tlabelId,\n" +
                "\tlabelName,\n" +
                "\tlabelValue,\n" +
                "\tcount(distinct userId) as custCount,\n" +
                audienceSize + "\nas audienceSize\n" +
                "\tfrom audience_profile_det\n" +
                "\twhere labelValue is not null" +
                "\tgroup by\n" +
                "\taudienceId,\n" +
                "\tdatasetId,\n" +
                "\tlabelId,\n" +
                "\tlabelName,\n" +
                "\tlabelValue");

        result.createOrReplaceTempView("audience_profile_sum");

        //3. 补充未知的枚举情况
        Dataset<Row> unknowSet = spark.sql("select\n" +
                "\taudienceId,\n" +
                "\tdatasetId,\n" +
                "\tlabelId,\n" +
                "\tlabelName,\n" +
                "\t\"未识别\" as labelValue,\n" +
                "\t(audienceSize- sum(custCount)) as custCount,\n" +
                "\taudienceSize\n" +
                "\t\n" +
                "from\n" +
                "\taudience_profile_sum\n" +
                "group by \n" +
                "\taudienceId,\n" +
                "\tdatasetId,\n" +
                "\tlabelId,\n" +
                "\tlabelName,\n" +
                "\taudienceSize");

        //4. 将最后结果union
        unknowSet.show(BigInteger.TEN.intValue());
        Dataset<Row> resultNew = result.union(unknowSet.filter("custCount > 0")).selectExpr("audienceId", "datasetId", "labelId", "labelName", "labelValue", "custCount", "audienceSize", "ROW_NUMBER() over(PARTITION BY labelId ORDER BY custCount desc) as rank").filter("rank <= " + PROFILE_COUNT_MAX).drop("rank");
        return resultNew.withColumn("id",
                functions.concat_ws("_", result.col("audienceId"), result.col("datasetId"),
                        result.col("labelId"), result.col("labelValue")));
    }

    /**
     * @throws
     * @title mergeFunc
     * @description 将标签数据行展开，并将多值性标签字段按行展开
     * @author cdxiongmei
     * @param: input
     * @updateTime 2021/8/10 上午10:58
     * @return: java.util.Iterator<org.apache.spark.sql.Row>
     */
    private Iterator<Row> mergeFunc(Iterator<Row> input, AudienceProfileBean bean) {
        List<Row> result = new ArrayList<>();
        while (input.hasNext()) {
            Row row = input.next();
            Long datasetId = Long.parseLong(row.getAs("datasetId").toString());
            //1. 获取标签id和标签分隔符对象
            List<JSONObject> labelFieldIds = bean.getLabelDetail().get(datasetId);
            //2. 标签字段进行列转行
            for (JSONObject labelInfo : labelFieldIds) {
                Long labelId = Long.parseLong(labelInfo.get("id").toString());
                Boolean isMulti = Boolean.valueOf(labelInfo.get("isMulti").toString());
                String labelValue = row.getAs(new StringBuilder("tag_").append(labelId).toString());
                //判断labelValue值是否符合规范
                boolean labelValueInvalid = StringUtils.isBlank(labelValue) || (!isMulti && StringUtils.length(labelValue) > ConfigProperties.LABEL_VALUE_LEN_MAX);
                if (labelValueInvalid) {
                    result.add(RowFactory.create(bean.getAudienceId(),
                            datasetId, labelId, bean.getLabelName().get(labelId), ConfigProperties.LABEL_VALUE_DEFAULT, row.getAs("user_id").toString()));
                } else if (isMulti) {
                    //3.多值标签字段按行展开
                    String[] items = labelValue.split("\\/");
                    for (String item : items) {
                        if (StringUtils.length(labelValue) > ConfigProperties.LABEL_VALUE_LEN_MAX) {
                            result.add(RowFactory.create(bean.getAudienceId(),
                                    datasetId, labelId, bean.getLabelName().get(labelId), ConfigProperties.LABEL_VALUE_DEFAULT, row.getAs("user_id").toString()));
                        } else {
                            result.add(RowFactory.create(bean.getAudienceId(),
                                    datasetId, labelId, bean.getLabelName().get(labelId), item, row.getAs("user_id").toString()));
                        }
                    }
                } else {
                    //正常枚举值
                    result.add(RowFactory.create(bean.getAudienceId(),
                            datasetId, labelId, bean.getLabelName().get(labelId), labelValue, row.getAs("user_id").toString()));
                }
            }
        }
        return result.iterator();
    }

    private void deleteEsData(AudienceProfileBean bean) {
        SqlCondition condDatasetId = new SqlCondition();
        condDatasetId.setColumnName("audienceId");
        condDatasetId.setParam1(bean.getAudienceId().toString());
        condDatasetId.setCompareOpType(ComparisonOperatorsEnum.EQUALITY);
        condDatasetId.setLogicalOperator(LogicalOperatorEnum.and);
        List<SqlCondition> conds = new ArrayList<SqlCondition>() {
            {
                add(condDatasetId);
            }
        };
        JSONObject dslJson = EsUtil.queryDslJSON(conds);
        EsCluster esCluster = new EsCluster();
        esCluster.setEsNodes(bean.getEsNode());
        esCluster.setEsPort(bean.getEsPort());
        esCluster.setUsername(bean.getEsUser());
        esCluster.setPassword(bean.getEsPass());
        esCluster.setEsIndex(bean.getEsIndex());
        EsUtil.deleteEsData(esCluster, dslJson);
        LOGGER.info("删除数据成功dslJson=" + dslJson.toJSONString());
    }

}

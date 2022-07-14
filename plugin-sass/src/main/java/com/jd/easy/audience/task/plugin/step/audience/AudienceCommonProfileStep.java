package com.jd.easy.audience.task.plugin.step.audience;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.jd.easy.audience.common.constant.LabelSourceEnum;
import com.jd.easy.audience.common.constant.LabelTypeEnum;
import com.jd.easy.audience.common.constant.LogicalOperatorEnum;
import com.jd.easy.audience.common.constant.NumberConstant;
import com.jd.easy.audience.common.exception.JobInterruptedException;
import com.jd.easy.audience.common.oss.JdCloudDataConfig;
import com.jd.easy.audience.common.oss.OssFileTypeEnum;
import com.jd.easy.audience.common.util.JdCloudOssUtil;
import com.jd.easy.audience.task.commonbean.bean.AudienceCommonProfileBean;
import com.jd.easy.audience.task.commonbean.bean.CommonLabelDetailBean;
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
import org.apache.spark.sql.api.java.UDF1;
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
 * 统一标签和自定义标签的人群透视分析
 *
 * @author xiongmei3
 * @date 2021/3/21
 */
public class AudienceCommonProfileStep extends StepCommon<Void> {
    /**
     * LOGGER
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(AudienceCommonProfileStep.class);
    /**
     * 标签透视枚举值上限限制
     */
    private static final Integer PROFILE_COUNT_MAX = NumberConstant.INT_50;

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
        AudienceCommonProfileBean bean = null;
        if (getStepBean() instanceof AudienceCommonProfileBean) {
            bean = (AudienceCommonProfileBean) getStepBean();
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
        Map<String, String> map = new HashMap<>(NumberConstant.INT_10);
        map.put("es.mapping.id", "id");
        JavaEsSparkSQL.saveToEs(calcAudienceLabelProfile(sparkSession, allLabelData, Long.valueOf(audienceDataset.count())), bean.getEsIndex(), map);
        return null;
    }

    /**
     * 从oss读取人群包明细数据
     *
     * @param spark
     * @return
     */
    @JsonIgnore
    public Dataset<Row> getAudienceData(SparkSession spark, AudienceCommonProfileBean bean) {
        //1. jsf协议的oss路径
        String jfsPath = bean.getBucket() + File.separator + URI.create(bean.getAudienceDataFile()).getPath();
        LOGGER.info("从oss获取人群包信息jfsPath={}", jfsPath);
        //2. 读取oss数据
        JdCloudDataConfig readconfig = new JdCloudDataConfig(bean.getEndpoint(), bean.getAccessKey(), bean.getSecretKey());
        readconfig.setOptions(new HashMap<String, String>() {
            {
                put("header", "true");
            }
        });
        readconfig.setClientType(ConfigProperties.getClientType());
        readconfig.setFileType(OssFileTypeEnum.csv);
        readconfig.setObjectKey(jfsPath);
        Dataset<Row> dataset = JdCloudOssUtil.readDatasetFromOss(spark, readconfig);

        //3. 校验人群包数据
        if (null == dataset || dataset.count() == BigInteger.ZERO.intValue()) {
            String str = "没有查到该人群生成结果数据,人群id=" + bean.getAudienceId();
            LOGGER.info(str);
            throw new JobInterruptedException(str, "No such audience package, audienceId = " + bean.getAudienceId());
        }
        LOGGER.info("audience size ={}", dataset.count());
        return dataset;
    }

    /**
     * 获取标签数据集标签明细数据
     *
     * @param spark
     * @return
     */
    @JsonIgnore
    public JavaRDD<Row> getLabelData(SparkSession spark, Dataset<Row> audienceData, AudienceCommonProfileBean bean) {
        //1.1 注册udf函数
        JavaRDD<Row> allLabelData = null;
        JavaRDD<Row> rowDataset = null;
        for (Long dataSetId : bean.getLabelDetails().keySet()) {
            List<CommonLabelDetailBean> labelDetails = bean.getLabelDetails().get(dataSetId);
            CommonLabelDetailBean labelFirst = labelDetails.get(NumberConstant.INT_0);
            if (LabelSourceEnum.custom.equals(labelFirst.getLabelSource())) {
                //自定义标签
                LOGGER.info("自定义标签模型datasetId={}, sourceUrl={}", dataSetId, bean.getCommonLabelTables().get(dataSetId));
                String jfsObjectKey = bean.getBucket() + File.separator + bean.getCommonLabelTables().get(dataSetId);
                JdCloudDataConfig readconfig = new JdCloudDataConfig(bean.getEndpoint(), bean.getAccessKey(), bean.getSecretKey());
                readconfig.setOptions(new HashMap<String, String>() {
                    {
                        put("header", "true");
                    }
                });
                readconfig.setClientType(ConfigProperties.getClientType());
                readconfig.setFileType(OssFileTypeEnum.csv);
                readconfig.setObjectKey(jfsObjectKey);
                Dataset<Row> labelDataset = JdCloudOssUtil.readDatasetFromOss(spark, readconfig);

                //2. 与人群join
                Dataset<Row> audienceLabelData = labelDataset.join(audienceData, "user_id");
//                        .withColumn("audienceId", functions.lit(bean.getAudienceId()))
//                        .withColumn("datasetId", functions.lit(dataSetId));
                LOGGER.info("join with audience result count={}", audienceLabelData.count());
                audienceLabelData.show(BigInteger.TEN.intValue());
                //3. 解析标签的tag信息（将多值的字段进行展开）
                rowDataset = audienceLabelData.mapPartitions((MapPartitionsFunction<Row, Row>) iterator ->
                        mergeFunc(iterator, labelDetails, bean.getAudienceId(), dataSetId), Encoders.bean(Row.class)).toJavaRDD();
            } else if (LabelSourceEnum.common.equals(labelFirst.getLabelSource())) {
                LOGGER.info("统一标签模型datasetId={}", dataSetId);
                Map<String, Long> labelIdsEnum = new HashMap<>(NumberConstant.INT_10);
                Map<String, Long> labelIdsMulti = new HashMap<>(NumberConstant.INT_10);
                //多值字段和枚举字段需要拆分label_value字段
                labelDetails.forEach(label -> {
                    if (LabelTypeEnum.multitype.equals(label.getLabelType())) {
                        labelIdsMulti.put(label.getLabelName(), label.getLabelId());
                    } else {
                        labelIdsEnum.put(label.getLabelName(), label.getLabelId());
                    }
                });
                spark.udf().register("getlabelid",
                        ((UDF1<String, Long>) labelName -> {
                            if (labelIdsEnum.containsKey(labelName)) {
                                return labelIdsEnum.get(labelName);
                            }
                            return labelIdsMulti.get(labelName);
                        }), DataTypes.LongType);
                String tableName = bean.getCommonLabelTables().get(dataSetId);
                String selectStr = " %s SELECT \n\t" +
                        "CAST(%s AS BIGINT) AS audience_id,\n\t" +
                        "CAST(%s AS BIGINT) AS dataset_id,\n\t" +
                        "user_id," +
                        "getlabelid(label_name) AS labelkey,\n\t" +
                        "label_name AS labelname,\n\t" +
                        "label_value AS labelval\n" +
                        "FROM %s WHERE set_id=\"%s\" AND label_name in ('%s') %s";
                List<String> unionList = new ArrayList<String>();
                if (!labelIdsEnum.isEmpty()) {
                    String enumSql = String.format(selectStr, "", bean.getAudienceId(), dataSetId, tableName, labelFirst.getSetId(), StringUtils.join(labelIdsEnum.keySet(), "\',\'"), "");
                    unionList.add(enumSql);
                }
                if (!labelIdsMulti.isEmpty()) {
                    String multiSql = String.format(selectStr, "SELECT audience_id, dataset_id, user_id, labelkey,labelname,labelval_new AS labelval FROM (", bean.getAudienceId(), dataSetId, tableName, labelFirst.getSetId(), StringUtils.join(labelIdsMulti.keySet(), "\',\'"), ")a lateral VIEW explode(split(labelval, \"\\\\/\")) multi_tb AS labelval_new");
                    unionList.add(multiSql);
                }
                String unionSql = StringUtils.join(unionList, " UNION ALL ");
                LOGGER.info("labelSQL:{}", unionSql);
                Dataset<Row> dataTemp = spark.sql(unionSql.toString()).join(audienceData, "user_id")
                        .select("audience_id", "dataset_id", "labelkey", "labelname", "labelval", "user_id");
                dataTemp.printSchema();
                rowDataset = dataTemp.toJavaRDD();
            } else {
                throw new JobInterruptedException("不支持的透视标签类型", "the label type is not supported");
            }
            //5. 结果数据集校验
            if (null == rowDataset || rowDataset.count() == NumberConstant.INT_0) {
                LOGGER.info("透视标签的人群数为0,人群id={}", bean.getAudienceId());
                continue;
//                throw new JobInterruptedException(str);
            }
            //4. 结果数据集合并
            if (null == allLabelData) {
                allLabelData = rowDataset;
            } else {
                allLabelData = allLabelData.union(rowDataset);
            }

        }
        if (null == allLabelData || allLabelData.count() == NumberConstant.INT_0) {
            String str = "透视标签的人群数为0,人群id=" + bean.getAudienceId();
            LOGGER.info(str);
            throw new JobInterruptedException(str, "No such audienceprofile result, audienceId = " + bean.getAudienceId());
        }
        return allLabelData;
    }

    /**
     * @throws
     * @title mergeFunc
     * @description 自定以标签模型，标签数据处理
     * @author cdxiongmei
     * @param: input
     * @param: labels
     * @updateTime 2021/8/12 下午4:53
     * @return: java.util.Iterator<org.apache.spark.sql.Row>
     */
    private Iterator<Row> mergeFunc(Iterator<Row> input, List<CommonLabelDetailBean> labels, Long audienceId, Long datasetId) {
        List<Row> result = new ArrayList<>();
        while (input.hasNext()) {
            Row row = input.next();
            //2. 标签字段进行列转行
            for (CommonLabelDetailBean labelInfo : labels) {
                Long labelId = labelInfo.getLabelId();
                Boolean isMulti = (LabelTypeEnum.multitype.equals(labelInfo.getLabelType()));
                String labelValue = row.getAs(new StringBuilder("tag_").append(labelId).toString()).toString();
                //判断labelValue值是否符合规范
                boolean labelValueInvalid = StringUtils.isBlank(labelValue) || (!isMulti && StringUtils.length(labelValue) > ConfigProperties.LABEL_VALUE_LEN_MAX);
                if (labelValueInvalid) {
                    result.add(RowFactory.create(audienceId,
                            datasetId, labelId, labelInfo.getLabelName(), ConfigProperties.LABEL_VALUE_DEFAULT, row.getAs("user_id").toString()));
                } else if (isMulti) {
                    //3.多值标签字段按行展开
                    String[] items = labelValue.split("\\/");
                    for (String item : items) {
                        if (StringUtils.length(labelValue) > ConfigProperties.LABEL_VALUE_LEN_MAX) {
                            result.add(RowFactory.create(audienceId,
                                    datasetId, labelId, labelInfo.getLabelName(), ConfigProperties.LABEL_VALUE_DEFAULT, row.getAs("user_id").toString()));
                        } else {
                            result.add(RowFactory.create(audienceId,
                                    datasetId, labelId, labelInfo.getLabelName(), item, row.getAs("user_id").toString()));
                        }
                    }
                } else {
                    //正常枚举值
                    result.add(RowFactory.create(audienceId,
                            datasetId, labelId, labelInfo.getLabelName(), labelValue, row.getAs("user_id").toString()));
                }
            }
        }
        return result.iterator();
    }

    /**
     * 计算人群标签
     */
    @JsonIgnore
    public Dataset<Row> calcAudienceLabelProfile(SparkSession spark, JavaRDD<Row> audienceDataset, Long audienceSize) {
        if (null == audienceDataset || audienceDataset.isEmpty()) {
            LOGGER.info("透视结果为0，不用保存到es中");
            return spark.emptyDataFrame();
        }
        //1. structfield信息
        List<StructField> fieldList = new ArrayList<>();
        fieldList.add(DataTypes.createStructField("audienceId", DataTypes.LongType, false));
        fieldList.add(DataTypes.createStructField("datasetId", DataTypes.LongType, false));
        fieldList.add(DataTypes.createStructField("labelId", DataTypes.LongType, false));
        fieldList.add(DataTypes.createStructField("labelName", DataTypes.StringType, false));
        fieldList.add(DataTypes.createStructField("labelValue", DataTypes.StringType, false));
        fieldList.add(DataTypes.createStructField("userId", DataTypes.StringType, false));
        StructType struct = DataTypes.createStructType(fieldList);
        Dataset<Row> dataRow = spark.createDataFrame(audienceDataset, struct);
        dataRow.printSchema();
        dataRow.show(NumberConstant.INT_10);
        dataRow.createOrReplaceTempView("audience_profile_det");

        //2. 聚合计算透视分析结果
        Dataset<Row> result = spark.sql("select\n" +
                "\taudienceId,\n" +
                "\tdatasetId,\n" +
                "\tlabelId,\n" +
                "\tlabelName,\n" +
                "\tlabelValue,\n" +
                "\tcast(count(distinct userId) as bigint) as custCount,\n" +
                audienceSize + "\nas audienceSize\n" +
                "\tfrom audience_profile_det\n" +
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
//        Dataset<Row> resultNew = result.union(unknowSet.filter("custCount > 0"));
        Dataset<Row> resultNew = result.union(unknowSet.filter("custCount > 0")).selectExpr("audienceId", "datasetId", "labelId", "labelName", "labelValue", "custCount", "audienceSize", "ROW_NUMBER() over(PARTITION BY labelId ORDER BY custCount desc) as rank").filter("rank <= " + PROFILE_COUNT_MAX).drop("rank");
        return resultNew.withColumn("id",
                functions.concat_ws("_", result.col("audienceId"), result.col("datasetId"),
                        result.col("labelId"), result.col("labelValue")));
    }

    private void deleteEsData(AudienceCommonProfileBean bean) {
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

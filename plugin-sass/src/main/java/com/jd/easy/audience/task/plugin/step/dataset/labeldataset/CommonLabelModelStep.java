package com.jd.easy.audience.task.plugin.step.dataset.labeldataset;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.jd.easy.audience.common.constant.LabelTypeEnum;
import com.jd.easy.audience.common.constant.LogicalOperatorEnum;
import com.jd.easy.audience.common.constant.NumberConstant;
import com.jd.easy.audience.common.constant.StringConstant;
import com.jd.easy.audience.common.exception.JobInterruptedException;
import com.jd.easy.audience.common.util.JdCloudOssUtil;
import com.jd.easy.audience.task.commonbean.bean.CommonLabelModelBean;
import com.jd.easy.audience.task.commonbean.util.SparkApplicationUtil;
import com.jd.easy.audience.task.driven.step.StepCommon;
import com.jd.easy.audience.task.driven.step.StepCommonBean;
import com.jd.easy.audience.task.plugin.property.ConfigProperties;
import com.jd.easy.audience.task.plugin.run.spark.SparkLauncherUtil;
import com.jd.easy.audience.task.plugin.util.es.EsUtil;
import com.jd.easy.audience.task.plugin.util.es.bean.EsCluster;
import com.jd.easy.audience.task.plugin.util.es.sqltool.ComparisonOperatorsEnum;
import com.jd.easy.audience.task.plugin.util.es.sqltool.SqlCondition;
import com.jd.jss.JingdongStorageService;
import com.typesafe.config.Config;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.reflect.ClassManifestFactory;

import java.math.BigInteger;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * @Author: xiongmei
 * @Date: 2021/8/12 上午10:51
 * @Description: 通用标签模型枚举计算
 * 1. 生成标签明细数据
 * 2. 生成该标签数据集的透视分析数据
 * 3. 获取该标签数据集中标签对应的枚举值
 */
public class CommonLabelModelStep extends StepCommon<Map<String, Object>> {

    /**
     * log工具
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(CommonLabelModelStep.class);
    /**
     * 枚举值总数的限制
     */
    private static final Integer ENUM_COUNT_MAX = NumberConstant.INT_500 + NumberConstant.INT_1;
    /**
     * 透视分析结果枚举数据的上限
     */
    private static final Integer PROFILE_COUNT_MAX = NumberConstant.INT_500;
    /**
     * 拆分数据库和表名（db.tablename）格式的正则表达式
     */
    private static final Pattern DB_TABLE_SPLIT = Pattern.compile("^([a-zA-Z0-9|_]{1,10}\\.[_|\\w]*){1}(\\.[_|\\w]*)");
    /*
     * 自增变量
     */
    private AtomicInteger atomicInteger = new AtomicInteger(NumberConstant.INT_1);
    @Override
    public Map<String, Object> run(Map<String, StepCommonBean> dependencies) {
        //1. 初始化
        LOGGER.info("file.encoding={}", System.getProperty("file.encoding"));
        System.getProperties().setProperty("file.encoding", "UTF-8");
        CommonLabelModelBean bean = null;
        if (getStepBean() instanceof CommonLabelModelBean) {
            bean = (CommonLabelModelBean) getStepBean();
        } else {
            throw new JobInterruptedException("bean 参数异常", "bean parameter is error!");
        }
        SparkSession spark = SparkLauncherUtil.buildSparkSessionWithESSettingsMultiNodes(bean.getEsNode(), bean.getEsPort(), bean.getEsUser(), bean.getEsPassword(), bean.getStepName());
        if (!spark.catalog().tableExists(bean.getLabelTable())) {
            String tableName = bean.getLabelTable().split("\\.")[NumberConstant.INT_1];
            throw new JobInterruptedException(tableName + " 表不存在，请重新检查", "Failure: The Table " + tableName + " is not exist!");
        }
        deleteEsData(bean);
        //清理oss数据
        JingdongStorageService jfsClient = JdCloudOssUtil.createJfsClient(bean.getEndpoint(), bean.getAccessKey(), bean.getSecretKey());
//        JdCloudOssUtil.deleteOssDir(jfsClient, bean.getBucket(), bean.getOssFilePath(), null);
        JdCloudOssUtil.deleteOssDir(jfsClient, bean.getBucket(), bean.getTagItemPath(), null);
        jfsClient.destroy();
        //2. 根据标签集配置信息生成sql拿到标签数据
        Dataset<Row> rawDataset = buildSql(spark, bean);

        //2.3 所有标签和标签值展开
        Dataset<Row> detailCut = explodeTags(spark, rawDataset, bean);
        //枚举分析
        saveLabelEnumData(detailCut, bean);
        //3.1 保存标签数据集画像--透视分析
        calLabelProfile(detailCut, bean);
        //3.2 获取各标签枚举明细
        return new HashMap<String, Object>(NumberConstant.INT_20);
    }

    /**
     * @Description 保存标签枚举信息到oss
     * @Author xiongmei3
     * @Date 2021/8/9 上午10:57
     * @Param
     * @Return
     * @Exception
     */
    private void saveLabelEnumData(Dataset<Row> detailCut, CommonLabelModelBean bean) {
        Map<String, List<String>> tagItem = new HashMap<>(NumberConstant.INT_20);
        detailCut.collectAsList().forEach(item -> {
            List<String> itemSet = tagItem.computeIfAbsent(item.getAs("key").toString(), k -> new ArrayList<>());
            itemSet.add(item.getAs("item").toString());

        });
        LOGGER.info(tagItem.toString());
        Map<String, Object> result = new HashMap<>(NumberConstant.INT_20);
        result.put("status", BigInteger.ONE.intValue());
        result.put("tagItem", tagItem);
        //4 标签枚举写入oss
        String objectStr = SparkApplicationUtil.encodeAfterObject2Json(result);
        JingdongStorageService jfsClient = JdCloudOssUtil.createJfsClient(bean.getEndpoint(), bean.getAccessKey(), bean.getSecretKey());
        JdCloudOssUtil.writeObjectToOss(jfsClient, objectStr, bean.getBucket(), JdCloudOssUtil.dealPath(bean.getTagItemPath()));
        jfsClient.destroy();
    }

    /**
     * 计算标签透视分析-暂时不支持
     *
     * @param detailCut
     */
    @Deprecated
    private void calLabelProfile(Dataset<Row> detailCut, CommonLabelModelBean bean) {
        //1. 计算每个标签的人群数-限制了每个标签的枚举数量
        Dataset<Row> profileRows = detailCut.filter("rank <= " + PROFILE_COUNT_MAX).select("key", "item", "count").withColumn("datasetId", functions.lit(bean.getDatasetId().toString()));
        Map<String, String> map = new HashMap<>(NumberConstant.INT_20);
        map.put("es.mapping.id", "id");
        JavaEsSparkSQL.saveToEs(profileRows.withColumn("id",
                functions.concat_ws(StringConstant.UNDERLINE, profileRows.col("datasetId"),
                        profileRows.col("key"), profileRows.col("item"))),
                bean.getEsIndex(), map);
    }

    /**
     * @throws
     * @title explodeTags
     * @description 枚举值需要进行行拆分
     * @author cdxiongmei
     * @param: spark
     * @param: rawDataset
     * @param: fieldDelimiterMap
     * @param: singleFields
     * @updateTime 2021/8/9 上午11:23
     * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     */
    public Dataset<Row> explodeTags(SparkSession spark, Dataset<Row> rawDataset, CommonLabelModelBean bean) {
        //2.2 标签类型整理
        final List<String> multiFields = new ArrayList<>();
        final List<String> enumFields = new ArrayList<String>();
        final Map<String, Long> labelIds = new HashMap<String, Long>(NumberConstant.INT_20);
        bean.getLabelDetails().forEach(item -> {
                    if (StringUtils.isNotBlank(item.getLabelName()) && LabelTypeEnum.multitype.getValue().equals(item.getLabelType().getValue())) {
                        multiFields.add(item.getLabelName());
                    }
                    if (StringUtils.isNotBlank(item.getLabelName()) && LabelTypeEnum.enumtype.getValue().equals(item.getLabelType().getValue())) {
                        //单值型的标签
                        enumFields.add(item.getLabelName());
                    }
                    labelIds.put(item.getLabelName(), Long.valueOf(item.getLabelId()));
                }
        );
        LOGGER.info("multiFields={},enumFields={},labelIds={}", multiFields.toString(), enumFields.toString(), labelIds);
        Broadcast<List<String>> broadcastmultiFields = spark.sparkContext().broadcast(multiFields, ClassManifestFactory.classType(List.class));
        Broadcast<List<String>> broadcastenumFields = spark.sparkContext().broadcast(enumFields, ClassManifestFactory.classType(List.class));
        Broadcast<Map<String, Long>> broadcastlabelIds = spark.sparkContext().broadcast(labelIds, ClassManifestFactory.classType(Map.class));
        ArrayList<StructField> fields = new ArrayList<StructField>();
        StructField field = null;
        field = DataTypes.createStructField("user_id", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("label_id", DataTypes.LongType, true);
        fields.add(field);
        field = DataTypes.createStructField("label_name", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("label_value", DataTypes.StringType, true);
        fields.add(field);

        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> detailRdd = rawDataset.repartition(BigInteger.TEN.intValue()).toJavaRDD().flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row line) throws Exception {
                List<Row> rowNew = new ArrayList<Row>();
                String userId = line.getAs("user_id").toString();
                String labelName = line.getAs("label_name").toString();
                String labelValue = line.getAs("label_value").toString();
                LOGGER.info("broadcastmultiFields:{},broadcastenumFields={},", broadcastmultiFields.value().toString(), broadcastenumFields.value().toString());
                if (!broadcastmultiFields.getValue().contains(labelName) && !broadcastenumFields.getValue().contains(labelName)) {
                    LOGGER.info("singlefield：{}", labelName);
                    return rowNew.iterator();
                }
                Long labelId = broadcastlabelIds.value().get(labelName);
                boolean labelValueInvalid = ((StringUtils.isBlank(labelValue)) || (broadcastenumFields.getValue().contains(labelName) && StringUtils.length(labelValue) > ConfigProperties.LABEL_VALUE_LEN_MAX));
                if (labelValueInvalid) {
                    //标签值为空值
                    LOGGER.info("写other：{}", labelName);
                    rowNew.add(RowFactory.create(userId, labelId, labelName, ConfigProperties.LABEL_VALUE_DEFAULT));
                } else if (broadcastmultiFields.getValue().contains(labelName)) {
                    //有些标签值可能是一个有分隔符的标签集合（多值标签）
                    LOGGER.info("multifield：{}", labelName);
                    for (String s : labelValue.toString().split("\\/")) {
                        if (StringUtils.length(s) > ConfigProperties.LABEL_VALUE_LEN_MAX) {
                            rowNew.add(RowFactory.create(userId, labelId, labelName, ConfigProperties.LABEL_VALUE_DEFAULT));
                        } else {
                            rowNew.add(RowFactory.create(userId, labelId, labelName, s));
                        }
                    }
                } else {
                    //此处针对标签值无需再次解析的数据
                    LOGGER.info("enum：{}", labelName);
                    rowNew.add(RowFactory.create(userId, labelId, labelName, labelValue));
                }
                return rowNew.iterator();
            }
        });
        LOGGER.info("detailRdd size={}", detailRdd.count());
        String tableView = new StringBuilder("data_view_").append(atomicInteger.getAndIncrement()).toString();
        Dataset<Row> data = spark.createDataFrame(detailRdd, schema);
        spark.createDataFrame(detailRdd, schema).createOrReplaceTempView(tableView);
        Config sqlTemplate = ConfigProperties.getsqlTemplate(this.getClass().getSimpleName(), new Exception().getStackTrace()[NumberConstant.INT_0].getMethodName());
        String sqlItem = MessageFormat.format(sqlTemplate.getString("labelProfile"), tableView);
        LOGGER.info("rank sql:{}", sqlItem);
        Dataset<Row> detailCut = spark.sql(sqlItem).filter("rank <= " + ENUM_COUNT_MAX);
        return detailCut;
    }

    /**
     * @throws
     * @title buildSql
     * @description 根据标签集配置信息生成sql
     * @author cdxiongmei
     * @param: spark
     * @param: bean
     * @updateTime 2021/8/12 上午10:53
     * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     */
    @JsonIgnore
    private Dataset<Row> buildSql(SparkSession spark, CommonLabelModelBean bean) {
        Config sqlTemplate = ConfigProperties.getsqlTemplate(this.getClass().getSimpleName(), new Exception().getStackTrace()[NumberConstant.INT_0].getMethodName());
        String sqlItem = MessageFormat.format(sqlTemplate.getString("commonLabelSearch"), bean.getLabelTable(), bean.getSetId());
//        StringBuilder sqlBuilder = new StringBuilder("SELECT user_id , label_name , label_value FROM ");
//        String labelTable = bean.getLabelTable();
//        sqlBuilder.append(labelTable).append(" WHERE set_id=\"").append(bean.getSetId()).append("\"");
        LOGGER.info("buildsql = {}", sqlItem);
        Dataset<Row> rawDataset = null;
        try {
            rawDataset = spark.sql(sqlItem);
        } catch (Exception e) {
            LOGGER.error("execute dataset sql error:" + e.getMessage(), e);
            throw new JobInterruptedException("统一标签配置信息错误", "Label Configuration occurs error");
        }
        if (null == rawDataset || rawDataset.count() == BigInteger.ZERO.intValue()) {
            throw new JobInterruptedException("统一标签模型计算结果为空", "The result of Label Dataset is empty");
        }
        return rawDataset;
    }
    private void deleteEsData(CommonLabelModelBean bean) {
        SqlCondition condDatasetId = new SqlCondition();
        condDatasetId.setColumnName("datasetId");
        condDatasetId.setParam1(bean.getDatasetId().toString());
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
        esCluster.setPassword(bean.getEsPassword());
        esCluster.setEsIndex(bean.getEsIndex());
        EsUtil.deleteEsData(esCluster, dslJson);
        LOGGER.info("删除数据成功dslJson=" + dslJson.toJSONString());

    }
}

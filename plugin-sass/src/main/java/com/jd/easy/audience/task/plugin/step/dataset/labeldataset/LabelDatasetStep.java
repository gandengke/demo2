package com.jd.easy.audience.task.plugin.step.dataset.labeldataset;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;
import com.jd.easy.audience.common.constant.LabelTypeEnum;
import com.jd.easy.audience.common.constant.LogicalOperatorEnum;
import com.jd.easy.audience.common.constant.NumberConstant;
import com.jd.easy.audience.common.constant.StringConstant;
import com.jd.easy.audience.common.exception.JobInterruptedException;
import com.jd.easy.audience.common.model.param.DatasetUserIdConf;
import com.jd.easy.audience.common.model.param.RelDatasetModel;
import com.jd.easy.audience.common.oss.OssClientTypeEnum;
import com.jd.easy.audience.common.util.JdCloudOssUtil;
import com.jd.easy.audience.task.commonbean.bean.LabelDatasetBean;
import com.jd.easy.audience.task.commonbean.bean.LabelDetailBean;
import com.jd.easy.audience.task.commonbean.util.SparkApplicationUtil;
import com.jd.easy.audience.task.driven.step.StepCommon;
import com.jd.easy.audience.task.driven.step.StepCommonBean;
import com.jd.easy.audience.task.plugin.property.ConfigProperties;
import com.jd.easy.audience.task.plugin.run.spark.SparkLauncherUtil;
import com.jd.easy.audience.task.plugin.util.CommonDealUtils;
import com.jd.easy.audience.task.plugin.util.SparkUdfUtil;
import com.jd.easy.audience.task.plugin.util.es.EsUtil;
import com.jd.easy.audience.task.plugin.util.es.bean.EsCluster;
import com.jd.easy.audience.task.plugin.util.es.sqltool.ComparisonOperatorsEnum;
import com.jd.easy.audience.task.plugin.util.es.sqltool.SqlCondition;
import com.jd.jss.JingdongStorageService;
import com.jd.jss.domain.ObjectSummary;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.text.MessageFormat;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author: xiongmei
 * @Date: 2021/7/9 上午10:51
 * @Description: 数据集计算任务，包含的逻辑：
 * 1. 生成标签明细数据
 * 2. 生成该标签数据集的透视分析数据
 * 3. 获取该标签数据集中标签对应的枚举值
 */
public class LabelDatasetStep extends StepCommon<Map<String, Object>> {

    /**
     * log工具
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(LabelDatasetStep.class);
    /**
     * 枚举值总数的限制
     */
    private static final Integer ENUM_COUNT_MAX = NumberConstant.INT_500;
    /**
     * 透视分析枚举值总数的限制
     */
    private static final Integer PROFILE_COUNT_MAX = NumberConstant.INT_500;
    /**
     * 拆分数据库和表名（db.tablename）格式的正则表达式
     */
    private static final Pattern DB_TABLE_SPLIT = Pattern.compile("^([a-zA-Z0-9|_]{1,20}\\.[_|\\w]*){1}(\\.[_|\\w]*)");

    @Override
    public Map<String, Object> run(Map<String, StepCommonBean> dependencies) throws IOException {
        //1. 初始化
        LOGGER.info("file.encoding={}", System.getProperty("file.encoding"));
        System.getProperties().setProperty("file.encoding", "UTF-8");
        LabelDatasetBean bean = null;
        //1.参数校验
        if (getStepBean() instanceof LabelDatasetBean) {
            bean = (LabelDatasetBean) getStepBean();
        } else {
            throw new JobInterruptedException("bean 参数异常", "bean parameter is error!");
        }
        SparkSession spark = SparkLauncherUtil.buildSparkSessionWithESSettingsMultiNodes(bean.getEsNode(), bean.getEsPort(), bean.getEsUser(), bean.getEsPassword(), bean.getStepName());
        String dbName = bean.getDataSource().get(0).getDatabaseName();
        spark.sql("use " + dbName);
        //2.清理es数据
        deleteEsData(bean);
        //3.判断表是否存在
        List<RelDatasetModel> sourceList = bean.getDataSource();
        for (int i = NumberConstant.INT_0; i < sourceList.size(); i++) {
            RelDatasetModel source = sourceList.get(i);
            if (!spark.catalog().tableExists(source.getDataTable())) {
                throw new JobInterruptedException(source.getDataTable() + " 表不存在，请重新检查", "Failure: The Table " + source.getDataTable() + " is not exist!");
            }
        }
        //4. 根据标签集配置信息生成数据集结果
        dealDataSet(spark, bean, false);
        return new HashMap<String, Object>(16);
    }

    /**
     * 加工标签明细数据到oss
     *
     * @param spark
     * @param bean
     * @param rawDataset
     */
    private void saveDetailCsvFile(SparkSession spark, LabelDatasetBean bean, Dataset<Row> rawDataset, boolean isTest) throws IOException {
        if (isTest) {
            LOGGER.info("标签明细数据加工完成");
            rawDataset.show();
            return;
        }
        LOGGER.info("write csv to oss path:filePath={}", bean.getOssFilePath());
        JingdongStorageService jfsClient = JdCloudOssUtil.createJfsClient(bean.getEndpoint(), bean.getAccessKey(), bean.getSecretKey());
        JdCloudOssUtil.deleteOssDir(jfsClient, bean.getBucket(), bean.getOssFilePath(), null);
        JdCloudOssUtil.deleteOssDir(jfsClient, bean.getBucket(), bean.getTagItemPath(), null);
        /**
         * 保存明细数据
         */
        String ossPath = JdCloudOssUtil.buildsOssPath(bean.getBucket(), bean.getOssFilePath(), OssClientTypeEnum.JFS);
        System.out.println("ossPath:" + ossPath);
        String hdfsPath = "dataset/tmp/" + bean.getDatasetId();
        cleanHdfsFile(hdfsPath, spark.sparkContext().hadoopConfiguration());
        rawDataset.write().option("header", "true").format("csv").save(hdfsPath);
        FileSystem fileSystem = FileSystem.get(spark.sparkContext().hadoopConfiguration());
        Long fileSize = getFileSize(new Path(hdfsPath), fileSystem);

        if (fileSize > 5000000000L) {
            System.out.println("大文件传输-以目录存储");
            initOssConfig(spark, bean.getEndpoint(), bean.getAccessKey(), bean.getSecretKey());
            rawDataset.repartition((int) (fileSize / 1000000000L)).write().option("header", "true").format("csv").save(ossPath);
            renameOssDir(jfsClient, bean.getBucket(), bean.getOssFilePath());
        } else {
            //单文件保存
            initOssConfig(spark, bean.getEndpoint(), bean.getAccessKey(), bean.getSecretKey());
            rawDataset.repartition(1).write().option("header", "true").format("csv").save(ossPath);
            //文件重命名
            JdCloudOssUtil.deleteOssDir(jfsClient, bean.getBucket(), JdCloudOssUtil.dealPath(bean.getOssFilePath()), bean.getOssFilePath() + File.separator + bean.getFileName());
        }
        cleanHdfsFile(hdfsPath, spark.sparkContext().hadoopConfiguration());
        jfsClient.destroy();
    }

    /**
     * 将多文件存储的csv文件放在最外层目录中
     *
     * @param jfsClient
     * @param bucketName
     * @param ossPath
     */
    public static void renameOssDir(JingdongStorageService jfsClient, String bucketName, String ossPath) {
        LOGGER.info("renameOssDir ready! ossPathPrefix=" + ossPath + File.separator);

        List<ObjectSummary> fileOld = jfsClient.bucket(bucketName).prefix(ossPath + File.separator).listObject().getObjectSummaries();
        for (ObjectSummary fileItem : fileOld) {
            LOGGER.info("renameOssDir current File, objectKey= " + fileItem.getKey());
            if (!fileItem.getKey().contains("_SUCCESS")) {
                //如果只是重命名
                String fileName = fileItem.getKey().substring(fileItem.getKey().lastIndexOf(File.separator) + 1);
                LOGGER.info("key " + fileItem.getKey() + " ready to " + ossPath + "/" + fileName);
                jfsClient.bucket(bucketName).object(ossPath + "/" + fileName).copyFrom(bucketName, fileItem.getKey()).replaceMetadata().copy();
            }
            jfsClient.bucket(bucketName).object(fileItem.getKey()).delete();

        }
    }

    /**
     * 使用oneid关联子句拼接
     *
     * @param oneIdInfoView
     * @return
     */
    private Dataset<Row> oneIdJoinSql(Dataset<Row> dataOrigin, List<DatasetUserIdConf> userIdConfList, Dataset<Row> oneIdInfoView) {
        Dataset<Row> nextOrigin = dataOrigin;
        Integer sourceCnt = userIdConfList.size();
        Dataset<Row> resultData = null;
        //1. 根据标示字段优先级加工与oneid的关联字段relation_field
        for (int i = 0; i < sourceCnt; i++) {
            DatasetUserIdConf oneIdConf = userIdConfList.get(i);
            String curuseridField = "user_" + oneIdConf.getUserIdType();
            Dataset<Row> curOrigin = nextOrigin.filter(curuseridField + " IS NOT NULL AND " + curuseridField + " !=\"\"");
            curOrigin = curOrigin.withColumn("relation_field", functions.concat(functions.col(curuseridField), functions.lit("_" + oneIdConf.getUserIdType())));
            nextOrigin = nextOrigin.filter(curuseridField + " IS  NULL OR " + curuseridField + " =\"\"");
            if (null == resultData) {
                resultData = curOrigin;
            } else if (!curOrigin.isEmpty()) {
                resultData = resultData.unionAll(curOrigin);
            }
        }
        //2. 和oneid表进行关联
        Dataset<Row> oneIdviewTmp = oneIdInfoView.selectExpr("one_id", "concat(`id`,'_' ,`id_type`) AS relation_field");
        Dataset<Row> dataset = resultData.join(oneIdviewTmp, "relation_field");
        dataset.cache();
        dataset.show(NumberConstant.INT_10);
        return dataset;
    }

    /**
     * @Description 保存标签枚举信息到oss
     * @Author xiongmei3
     * @Date 2021/7/9 上午10:57
     * @Param
     * @Return
     * @Exception
     */
    private void saveLabelEnumData(Dataset<Row> detailCut, LabelDatasetBean bean, boolean isTest) {
        Map<String, List<String>> tagItem = new HashMap<>(16);
        detailCut.collectAsList().forEach(item -> {
            List<String> itemSet = tagItem.computeIfAbsent(item.getAs("key").toString(), k -> new ArrayList<>());
            itemSet.add(item.getAs("item"));

        });
        if (isTest) {
            LOGGER.info("标签枚举数据加工完成" + tagItem.toString());
            return;
        }
        LOGGER.info(tagItem.toString());
        Map<String, Object> result = new HashMap<>(16);
        result.put("tagItem", tagItem);
        result.put("status", BigInteger.ONE.intValue());

        //4 标签枚举写入oss
        String objectStr = SparkApplicationUtil.encodeAfterObject2Json(result);
        JingdongStorageService jfsClient = JdCloudOssUtil.createJfsClient(bean.getEndpoint(), bean.getAccessKey(), bean.getSecretKey());
        JdCloudOssUtil.writeObjectToOss(jfsClient, objectStr, bean.getBucket(), bean.getTagItemPath() + "/tagItem");
        jfsClient.destroy();
    }

    /**
     * 计算标签透视分析
     *
     * @param detailCut
     */
    private void calLabelProfile(Dataset<Row> detailCut, LabelDatasetBean bean, boolean isTest) {
        //1. 计算每个标签的人群数-限制了每个标签的枚举数量
        Dataset<Row> profileRows = detailCut.filter("rank <= " + PROFILE_COUNT_MAX).select("key", "item", "count").withColumn("datasetId", functions.lit(bean.getDatasetId().toString()));
        Map<String, String> map = new HashMap<>(16);
        map.put("es.mapping.id", "id");
        Dataset<Row> profileRet = profileRows.withColumn("id",
                functions.concat_ws("_", profileRows.col("datasetId"),
                        profileRows.col("key"), profileRows.col("item")));
        if (isTest) {
            LOGGER.info("标签明细数据加工完成");
            profileRet.show();
            return;
        }
        JavaEsSparkSQL.saveToEs(profileRet,
                bean.getEsIndex(), map);

    }

    /**
     * @throws
     * @title explodeTags
     * @description 加工枚举信息
     * @author cdxiongmei
     * @param: spark
     * @param: rawDataset
     * @param: fieldDelimiterMap
     * @param: singleFields
     * @updateTime 2021/7/9 上午11:23
     * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     */
    private Dataset<Row> explodeTags(SparkSession spark, Dataset<Row> rawDataset, List<LabelDetailBean> labels) {
        //1 标签类型整理
        final Map<Long, String> fieldDelimiterMap = new HashMap<>(16);
        final List<Long> singleFields = new ArrayList<Long>();
        labels.forEach(item -> {
                    if (StringUtils.isNotBlank(item.getFieldName()) && item.getLabelType().getValue().equals(LabelTypeEnum.multitype.getValue())) {
                        fieldDelimiterMap.put(item.getLabelId(), item.getFieldDelimiter());
                    }
                    if (StringUtils.isNotBlank(item.getFieldName()) && item.getLabelType().getValue().equals(LabelTypeEnum.singletype.getValue())) {
                        //单值型的标签
                        singleFields.add(item.getLabelId());
                    }
                }
        );
        LOGGER.info("fieldDelimiterMap={}", fieldDelimiterMap.toString());

        ArrayList<StructField> fields = new ArrayList<StructField>();
        StructField field = null;
        field = DataTypes.createStructField("user_id", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("label_id", DataTypes.LongType, true);
        fields.add(field);
        field = DataTypes.createStructField("label_value", DataTypes.StringType, true);
        fields.add(field);

        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> detailRdd = rawDataset.repartition(BigInteger.TEN.intValue()).toJavaRDD().flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row line) throws Exception {
                List<Row> rowNew = new ArrayList<Row>();
                for (StructField field : line.schema().fields()) {
                    if (!field.name().startsWith("tag_") || field.name().split(StringConstant.UNDERLINE).length != NumberConstant.INT_2) {
                        //不是标签字段
                        continue;
                    }
                    Object obj = line.getAs(field.name());
                    Long labelId = Long.parseLong(field.name().split("_")[BigInteger.ONE.intValue()]);
                    String delimiterValue = fieldDelimiterMap.get(labelId);
                    boolean labelValueInvalid = ((null == obj) || (StringConstant.EMPTY.equals(obj)) || (null == delimiterValue && StringUtils.length(obj.toString()) > ConfigProperties.LABEL_VALUE_LEN_MAX));
                    if (singleFields.contains(labelId)) {
                        //单值性不进行统计标签的统计
                        continue;
                    } else if (labelValueInvalid) {
                        //标签值为空值
                        rowNew.add(RowFactory.create(line.getAs("user_id"), labelId, ConfigProperties.LABEL_VALUE_DEFAULT));
                    } else if (null != delimiterValue) {
                        //有些标签值可能是一个有分隔符的标签集合（多值标签）
                        for (String s : obj.toString().split("/")) {
                            if (StringUtils.length(s) > ConfigProperties.LABEL_VALUE_LEN_MAX) {
                                rowNew.add(RowFactory.create(line.getAs("user_id"), labelId, ConfigProperties.LABEL_VALUE_DEFAULT));
                            } else {
                                rowNew.add(RowFactory.create(line.getAs("user_id"), labelId, s));
                            }
                        }
                    } else {
                        //此处针对标签值无需再次解析的数据
                        rowNew.add(RowFactory.create(line.getAs("user_id"), labelId, obj.toString()));
                    }

                }
                return rowNew.iterator();
            }
        });
        String tableView = "data_view";
        spark.createDataFrame(detailRdd, schema).createOrReplaceTempView(tableView);
        String sqlItem = "select\n" +
                "\tcount,\n" +
                "\tkey,\n" +
                "\titem,\n" +
                "\trow_number() over(partition by key order by count desc) as rank\n" +
                "from\n" +
                "\t(\n" +
                "\t\tselect\n" +
                "\t\t\tcount(distinct user_id) as count,\n" +
                "\t\t\tlabel_id as key,\n" +
                "\t\t\tlabel_value as item\n" +
                "\t\tfrom\n" +
                "\t\t\t" + tableView + "\n" +
                "\t\tgroup by\n" +
                "\t\t\tlabel_id,\n" +
                "\t\t\tlabel_value\n" +
                "\t)\n" +
                "\ta";
        LOGGER.info("rank sql:{}", sqlItem);
        Dataset<Row> detailCut = spark.sql(sqlItem).filter("rank <= " + ENUM_COUNT_MAX);
        detailCut.show(NumberConstant.INT_10);
        return detailCut;
    }

    public String getUserIdAlias(String userIdField, Map<String, String> tableAlias, Boolean isOneIdJoin, Boolean isOneIdRet) {
        String userIdFiledAlias = "";
        if (isOneIdJoin && isOneIdRet) {
            //使用oneID关联且使用oneID结果
            userIdFiledAlias = "tbl_0.one_id";
        } else if (isOneIdJoin) {
            //使用的是oneID做关联，但是结果是任意表的任意字段
            String userIdTable = userIdField.split("\\.")[1];
            userIdFiledAlias = tableAlias.get(userIdTable) + StringConstant.DOTMARK + "user_id";
        } else if (isOneIdRet) {
            //无需使用oneID关联，只需使用oneID生成结果
            userIdFiledAlias = ConfigProperties.ONEID_DETAIL_TB + ".one_id";
        } else {
            //普通字段关联，且普通字段作为用户标示
            String userIdTable = userIdField.split("\\.")[1];
            userIdFiledAlias = tableAlias.get(userIdTable) + StringConstant.DOTMARK + userIdField.split("\\.")[2];
        }
        return userIdFiledAlias;
    }

    /**
     * 根据标签集配置信息生成sql
     *
     * @param spark
     * @param bean
     * @return
     */
    @JsonIgnore
    public void dealDataSet(SparkSession spark, LabelDatasetBean bean, Boolean isTest) throws IOException {
        //1. 数据源的别名处理
        Map<String, String> tableAlias = new HashMap<>(NumberConstant.INT_20);
        for (int i = 0; i < bean.getDataSource().size(); i++) {
            int index = i;
            RelDatasetModel source = bean.getDataSource().get(i);
            String tableName = source.getDataTable().trim();
            tableAlias.computeIfAbsent(tableName, k -> "tbl_" + index);
        }
        LOGGER.info("tableAlias:" + tableAlias);

        //2. 是否使用oneID做表关联/是否使用oneID进行返回
        Boolean isOneIdJoin = bean.getDataSource().stream().anyMatch(RelDatasetModel::isUseOneId);
        Boolean isOneIdRet = bean.isUseOneId();
        //3. 用户标示字段处理
        String sqlHeaderTemplate = "SELECT CAST({0} AS STRING) AS user_id,''{1}'' AS id_type,";
        String userIdFiledAlias = getUserIdAlias(bean.getUserIdField(), tableAlias, isOneIdJoin, isOneIdRet);
        StringBuilder sqlBuilder = new StringBuilder(MessageFormat.format(sqlHeaderTemplate, userIdFiledAlias, bean.getUserType()));
        //4. 子表处理
        if (isOneIdJoin) {
            //eg: [tag_1, tag_3, tag_2, tag_4]
            List<String> headFields = bean.getLabels().stream().map(label -> "tag_" + label.getLabelId()).collect(Collectors.toList());
            sqlBuilder.append(StringUtils.join(headFields, StringConstant.COMMA) + " FROM ");
            //4.1 oneID的关联表达式
            sqlBuilder.append(buildSqlByOneId(spark, tableAlias, bean, isTest));
        } else {
            //4.2 使用普通字段关联
            sqlBuilder.append(buildSqlByNormal(tableAlias, bean, isTest));
        }

        LOGGER.error("buildSql = " + sqlBuilder.toString());
        Dataset<Row> rawDataset = null;
        try {
            rawDataset = spark.sql(sqlBuilder.toString());
        } catch (Exception e) {
            LOGGER.error("execute dataset sql error:" + e.getMessage(), e);
            throw new JobInterruptedException("标签配置信息错误", "Label Configuration occurs error");
        }
        if (null == rawDataset || rawDataset.count() == BigInteger.ZERO.intValue()) {
            throw new JobInterruptedException("标签结果数据为空", "The result of Label Dataset is empty");
        }
        Dataset<Row> rawDatasetFilter = rawDataset.filter("user_id is not null and user_id !=''");
        //5. 保存标签明细
        saveDetailCsvFile(spark, bean, rawDatasetFilter, isTest);
        //6. 所有标签和标签值展开
        Dataset<Row> detailCut = explodeTags(spark, rawDatasetFilter, bean.getLabels());
        //7. 枚举分析
        saveLabelEnumData(detailCut, bean, isTest);
        //8. 保存标签数据集画像--透视分析
        calLabelProfile(detailCut, bean, isTest);
    }

    /**
     * 同标识类型的数据源
     *
     * @param tableAlias
     * @param bean
     * @return
     */
    private String buildSqlByNormal(Map<String, String> tableAlias, LabelDatasetBean bean, boolean isTest) {
        //1. 表的数据源信息
        Map<String, RelDatasetModel> tableMap = bean.getDataSource().stream().collect(Collectors.toMap(RelDatasetModel::getDataTable, Function.identity()));
        //2. 标签字段标准化
        List<String> headFields = Lists.newArrayList();
        bean.getLabels().forEach(item -> {
            String tableName = item.getTable().trim();
            //如果存在一个表中取两个标签，则以第一次的别名为表别名
            String tableAliasName = tableAlias.get(tableName);
            String columnExt = "";
            if (StringUtils.isNotBlank(item.getFieldDelimiter())) {
                String fieldDelimiter = CommonDealUtils.parseFieldDelimter(item.getFieldDelimiter());
                columnExt = "CAST(REGEXP_REPLACE(" + tableAliasName + StringConstant.DOTMARK + item.getFieldName().trim() + ",'" + fieldDelimiter + "', \"\\/\") AS string) AS tag_" + item.getLabelId();
                System.out.println("多值字段处理：" + columnExt);
            } else if (StringUtils.isNotBlank(item.getFieldName())) {
                columnExt = "CAST(" + tableAliasName + StringConstant.DOTMARK + item.getFieldName().trim() + " AS string) AS tag_" + item.getLabelId();
            }
            headFields.add(columnExt);
        });

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(String.join(StringConstant.COMMA, headFields));
        sqlBuilder.append(" FROM ");
        //3. 标签源表直接进行关联
        String[] tableList = tableAlias.keySet().toArray(new String[NumberConstant.INT_0]);
        for (int i = BigInteger.ZERO.intValue(); i < tableList.length; i++) {
            String tableName = tableList[i];
            if (i > BigInteger.ZERO.intValue()) {
                sqlBuilder.append(" JOIN ");
                sqlBuilder.append(tableName);
                sqlBuilder.append(" AS ");
                sqlBuilder.append(tableAlias.get(tableName));
                sqlBuilder.append(" ON ").append(tableAlias.get(tableName)).append(".");
                sqlBuilder.append(tableMap.get(tableName).getRelationField());

                String preTableName = tableList[i - BigInteger.ONE.intValue()];
                sqlBuilder.append(" = ").append(tableAlias.get(preTableName)).append(".");
                sqlBuilder.append(tableMap.get(preTableName).getRelationField());
            } else {
                sqlBuilder.append(tableName).append(" AS ").append(tableAlias.get(tableName));
            }
        }
        //4. 关联oneid获取用户标示
        Boolean isOneIdRet = bean.isUseOneId();
        if (isOneIdRet) {
            Map<String, List<DatasetUserIdConf>> config = getOneIdConfigInfo(bean, isTest);
            String tableName = config.keySet().toArray()[0].toString();
            DatasetUserIdConf configCur = config.get(tableName).get(0);
            sqlBuilder.append(" JOIN ");
            sqlBuilder.append(ConfigProperties.ONEID_DETAIL_TB);
            sqlBuilder.append(" ON " + ConfigProperties.ONEID_DETAIL_TB + ".id = ").append(tableAlias.get(tableName)).append("." + configCur.getUserField());
            sqlBuilder.append(" WHERE " + ConfigProperties.ONEID_DETAIL_TB + ".id_type=" + configCur.getUserIdType());
        }
        return sqlBuilder.toString();
    }

    /**
     * 获取对应表的oneid配置信息
     *
     * @param bean
     * @param isTest
     * @return
     */
    private Map<String, List<DatasetUserIdConf>> getOneIdConfigInfo(LabelDatasetBean bean, Boolean isTest) {
        Map<String, List<DatasetUserIdConf>> config = new HashMap<>();
        if (isTest) {
            config.put("one_id_label_8_9", new ArrayList<DatasetUserIdConf>() {
                {
                    DatasetUserIdConf conf1 = new DatasetUserIdConf();
                    conf1.setUserIdType(8L);
                    conf1.setUserField("user_8");
                    DatasetUserIdConf conf2 = new DatasetUserIdConf();
                    conf2.setUserIdType(8L);
                    conf2.setUserField("user_9");
                    add(conf1);
                    add(conf2);
                }
            });
            config.put("one_id_label_9_10", new ArrayList<DatasetUserIdConf>() {
                {
                    DatasetUserIdConf conf1 = new DatasetUserIdConf();
                    conf1.setUserIdType(9L);
                    conf1.setUserField("user_9");
                    DatasetUserIdConf conf2 = new DatasetUserIdConf();
                    conf2.setUserIdType(10L);
                    conf2.setUserField("user_10");
                    add(conf1);
                    add(conf2);
                }
            });
        } else {
            String userIdTb = "";
            if (bean.isUseOneId()) {
                userIdTb = bean.getUserIdField().replaceAll("\\.OneId", "");
            }
            config = CommonDealUtils.getSourceOneIdConfig(bean.getDataSource(), userIdTb);
        }
        return config;
    }

    private List<String> getFieldList(List<LabelDetailBean> labels, List<DatasetUserIdConf> userIdConfList) {
        List<String> fields = new ArrayList<String>();
        //1. 标准化标签字段
        labels.forEach(item -> {
            String columnExt = "";
            if (item.getLabelType().getValue().equalsIgnoreCase(LabelTypeEnum.multitype.getValue()) && StringUtils.isNotBlank(item.getFieldDelimiter())) {
                // 多值型标签
                String fieldDelimiter = CommonDealUtils.parseFieldDelimter(item.getFieldDelimiter());
                columnExt = "CAST(REGEXP_REPLACE(" + item.getFieldName().trim() + ",'" + fieldDelimiter + "', \"\\/\") AS string) AS tag_" + item.getLabelId();
            } else if (item.getLabelType().getValue().equalsIgnoreCase(LabelTypeEnum.enumtype.getValue())) {
                columnExt = "CAST(" + item.getFieldName().trim() + " AS string) AS tag_" + item.getLabelId();
            }
            fields.add(columnExt);
        });
        if (null == userIdConfList) {
            return fields;
        }
        //2. 标准化oneid的用户标示字段，聚合同种类型的标示字段，保证取到有值的用户标示
        List<String> userIdFields = SparkUdfUtil.getUserIdsFromOneId(userIdConfList);
        fields.addAll(userIdFields);
        return fields;
    }

    /**
     * 启用oneid拼接查询sql
     *
     * @param tableAlias
     * @return
     */
    private String buildSqlByOneId(SparkSession sparkSession, Map<String, String> tableAlias, LabelDatasetBean bean, boolean isTest) throws IOException {
        //1. 校验oneID表的有效性
        if (!sparkSession.catalog().tableExists(ConfigProperties.ONEID_DETAIL_TB)) {
            LOGGER.error("oneid配置表：" + ConfigProperties.ONEID_DETAIL_TB);
            throw new JobInterruptedException("未配置one_id,没有可用的one_id数据", "You haven't config oneId source");
        }

        //2. 获取oneID表数据
        LOGGER.error("读取oneid数据");
        Dataset<Row> oneIdInfoView = sparkSession.read().table(ConfigProperties.ONEID_DETAIL_TB);
        oneIdInfoView.cache();

        //3. 用户标示字段
        String mainTable = bean.getUserIdField().split("\\.")[1];
        String userIdFieldPure = bean.getUserIdField().split("\\.")[2];
        //4. 获取oneID配置
        Map<String, List<DatasetUserIdConf>> config = getOneIdConfigInfo(bean, isTest);
        //5. 数据源信息
        Map<String, RelDatasetModel> sourceMap = bean.getDataSource().stream().collect(Collectors.toMap(RelDatasetModel::getDataTable, Function.identity()));

        String[] tableList = tableAlias.keySet().toArray(new String[NumberConstant.INT_0]);
        StringBuilder sqlBuilder = new StringBuilder();
        //6. 依次处理每个数据源的临时视图
        for (int i = BigInteger.ZERO.intValue(); i < tableList.length; i++) {
            String tableName = tableList[i];
            List<LabelDetailBean> labels = bean.getLabels().stream().filter(label -> label.getTable().equalsIgnoreCase(tableName)).collect(Collectors.toList());
            //6.1 加工表需要标准化的字段（标签和用户字段）
            List<String> fields = getFieldList(labels, config.get(tableName));
            //6.2 标准化user_id字段
            if (tableName.equalsIgnoreCase(mainTable) && !bean.isUseOneId()) {
                fields.add("CAST(" + userIdFieldPure + " AS string) AS user_id");
            }
            if (!sourceMap.get(tableName).isUseOneId()) {
                //6.3 无需使用oneid关联,需要重命名关联字段
                fields.add("CAST(" + sourceMap.get(tableName).getRelationField() + " AS string) AS one_id");
            }
            String selectSql = "SELECT " + StringUtils.join(fields, StringConstant.COMMA) + " FROM " + tableName;
            LOGGER.error("searchsql:" + selectSql);
            Dataset<Row> dataOrigin = sparkSession.sql(selectSql);
            //6.4 生成每个数据源处理后的临时视图
            if (sourceMap.get(tableName).isUseOneId()) {
                Dataset<Row> dataset = oneIdJoinSql(dataOrigin, config.get(tableName), oneIdInfoView);
                LOGGER.error("生成临时表：" + tableAlias.get(tableName));
                dataset.createOrReplaceTempView(tableAlias.get(tableName));
            } else {
                dataOrigin.createOrReplaceTempView(tableAlias.get(tableName));
            }
            //6.5 关联多个子表的临时视图
            if (i > BigInteger.ZERO.intValue()) {
                String preTableName = tableList[i - BigInteger.ONE.intValue()];
                sqlBuilder.append(" JOIN ");
                sqlBuilder.append(tableAlias.get(tableName));
                sqlBuilder.append(" ON ").append(tableAlias.get(tableName)).append(StringConstant.DOTMARK);
                sqlBuilder.append("one_id");
                sqlBuilder.append(" = ").append(tableAlias.get(preTableName)).append(".one_id");

            } else {
                sqlBuilder.append(tableAlias.get(tableName));
            }

        }
        return sqlBuilder.toString();
    }

    /**
     * 删除es数据
     *
     * @param bean
     */
    private void deleteEsData(LabelDatasetBean bean) {
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

    private void cleanHdfsFile(String filePath, Configuration conf) {
        if (filePath == null || conf == null) {
            return;
        }
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(conf);
            fileSystem.delete(new Path(filePath), true);
        } catch (IOException e) {
            System.out.println("delete the file error:" + e.getMessage());
        }
    }

    private Long getFileSize(Path path, FileSystem fileSystem) {
        try {
            FileStatus[] instatus = fileSystem.listStatus(path);
            Long fileSize = 0L;
            for (FileStatus fileEle :
                    instatus) {
                if (fileEle.isDirectory()) {
                    Long sizeTmp = getFileSize(fileEle.getPath(), fileSystem);
                    fileSize = fileSize + sizeTmp;
                } else {
                    fileSize = fileSize + fileEle.getLen();
                }
            }
            return fileSize;
        } catch (Exception ex) {
            System.out.println("获取表信息有异常" + ex);
            return 0L;
        }
    }

    private void initOssConfig(SparkSession sparkSession, String endpoint, String accessKey, String secretKey) {
        Configuration configuration = sparkSession.sparkContext().hadoopConfiguration();
        configuration.set("fs.jfs.impl", "com.jd.easy.audience.task.plugin.util.jfs.JFSFileSystem");
        configuration.set("fs.jfs.endPoint", endpoint);
        configuration.set("fs.jfs.accessKey", accessKey);
        configuration.set("fs.jfs.secretKey", secretKey);
        configuration.set("fs.jfs.block.size", "67108864");
        configuration.set("fs.jfs.skip.size", "1048576");
    }
}

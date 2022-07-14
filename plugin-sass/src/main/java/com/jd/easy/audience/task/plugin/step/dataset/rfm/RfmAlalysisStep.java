package com.jd.easy.audience.task.plugin.step.dataset.rfm;

import com.alibaba.fastjson.JSONObject;
import com.jd.easy.audience.common.constant.LogicalOperatorEnum;
import com.jd.easy.audience.common.constant.NumberConstant;
import com.jd.easy.audience.common.constant.RFMDataTypeEnum;
import com.jd.easy.audience.common.constant.StringConstant;
import com.jd.easy.audience.common.exception.JobInterruptedException;
import com.jd.easy.audience.common.model.param.DatasetUserIdConf;
import com.jd.easy.audience.common.model.param.RelDatasetModel;
import com.jd.easy.audience.common.util.JdCloudOssUtil;
import com.jd.easy.audience.task.commonbean.bean.RfmAlalysisBean;
import com.jd.easy.audience.task.driven.step.StepCommon;
import com.jd.easy.audience.task.driven.step.StepCommonBean;
import com.jd.easy.audience.task.generator.util.DateUtil;
import com.jd.easy.audience.task.plugin.property.ConfigProperties;
import com.jd.easy.audience.task.plugin.run.spark.SparkLauncherUtil;
import com.jd.easy.audience.task.plugin.step.dataset.rfm.algorithms.RfmAnalysis;
import com.jd.easy.audience.task.plugin.step.dataset.rfm.algorithms.taskInfo.RfmAnalysisConfigInfo;
import com.jd.easy.audience.task.plugin.step.dataset.rfm.algorithms.taskInfo.RfmAnalysisTaskNew;
import com.jd.easy.audience.task.plugin.util.CommonDealUtils;
import com.jd.easy.audience.task.plugin.util.SparkUdfUtil;
import com.jd.easy.audience.task.plugin.util.es.EsUtil;
import com.jd.easy.audience.task.plugin.util.es.bean.EsCluster;
import com.jd.easy.audience.task.plugin.util.es.sqltool.ComparisonOperatorsEnum;
import com.jd.easy.audience.task.plugin.util.es.sqltool.SqlCondition;
import com.jd.jss.JingdongStorageService;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author cdxiongmei
 * @title RfmAlalysisStep
 * @description 根据配置规则生成parquet文件到oss
 * @date 2021/8/10 下午7:33
 * @throws
 */
public class RfmAlalysisStep extends StepCommon<Void> {
    /**
     * LOG对象
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(RfmAlalysisStep.class);
    /**
     * 时间分区字段偏移的天数
     */
    private static final int DT_FILTER_DAYS = NumberConstant.INT_60;
    /**
     * 库表拆分正则表达式
     */
    private static final Pattern DB_TABLENAME_SPLIT = Pattern.compile("^([a-zA-Z0-9|_]{0,10}\\.[_|\\w]*){1}(\\.[_|\\w]*)");
    /**
     * 用户数据的schema
     */
    List<StructField> userfields = new LinkedList<StructField>() {
        {
            add(DataTypes.createStructField("user_log_acct", DataTypes.StringType, true));
            add(DataTypes.createStructField("frequency", DataTypes.IntegerType, true));
            add(DataTypes.createStructField("monetary", DataTypes.DoubleType, true));
            add(DataTypes.createStructField("amount", DataTypes.IntegerType, true));
            add(DataTypes.createStructField("channel", DataTypes.createArrayType(DataTypes.StringType, true), true));
            add(DataTypes.createStructField("first_dt", DataTypes.StringType, true));
            add(DataTypes.createStructField("latest_dt", DataTypes.StringType, true));
        }
    };
    /**
     * 交易数据的schema
     */
    List<StructField> dealfields = new LinkedList<StructField>() {
        {
            add(DataTypes.createStructField("user_log_acct", DataTypes.StringType, true));
            add(DataTypes.createStructField("parent_sale_ord_id", DataTypes.StringType, true));
            add(DataTypes.createStructField("monetary", DataTypes.DoubleType, true));
            add(DataTypes.createStructField("amount", DataTypes.IntegerType, true));
            add(DataTypes.createStructField("channel", DataTypes.StringType, true));
            add(DataTypes.createStructField("dt", DataTypes.StringType, true));
        }
    };

    @Override
    public Void run(Map<String, StepCommonBean> dependencies) {
        RfmAlalysisBean bean = null;
        if (getStepBean() instanceof RfmAlalysisBean) {
            bean = (RfmAlalysisBean) getStepBean();
        } else {
            throw new JobInterruptedException("bean 参数异常", "bean parameter occurs errors;");
        }
        //1.启动sparksession
        SparkSession spark = SparkLauncherUtil.buildSparkSessionWithESSettingsMultiNodes(bean.getRfmAnalysisConfig().getEsNode(), bean.getRfmAnalysisConfig().getEsPort(), bean.getRfmAnalysisConfig().getEsUser(), bean.getRfmAnalysisConfig().getEsPassword(), bean.getStepName());
        List<RelDatasetModel> tbList = bean.getRelDataset().getData();
        tbList.forEach(tb -> {
            if (!spark.catalog().tableExists(tb.getDatabaseName(), tb.getDataTable())) {
                throw new JobInterruptedException(tb.getDataTable() + " 表不存在，请重新检查", "Failure: The Table " + tb.getDataTable() + " is not exist!");
            }
        });
        //2.清理数据
        deleteEsData(bean);
        JingdongStorageService jfsClient = JdCloudOssUtil.createJfsClient(bean.getRfmAnalysisConfig().getJfsEndPoint(), bean.getRfmAnalysisConfig().getAccessKey(), bean.getRfmAnalysisConfig().getSecretKey());
        JdCloudOssUtil.deleteOssDir(jfsClient, bean.getRfmAnalysisConfig().getJfsBucket(), bean.getOutputPath(), null);
        jfsClient.destroy();

        spark.sql("use " + tbList.get(0).getDatabaseName());
        //3. 获取数据
        Dataset<Row> dataset = buildSql(spark, bean, false).repartition(NumberConstant.INT_10);

        //4.模型调用
        computeAlalysis(spark, dataset, bean);
        return null;
    }

    /**
     * @throws
     * @title computeAlalysis
     * @description 参数封装，调用算法模块
     * @author cdxiongmei
     * @param: spark
     * @param: dataSet
     * @param: bean
     * @updateTime 2021/8/10 下午8:13
     */
    private void computeAlalysis(SparkSession spark, Dataset<Row> dataSet, RfmAlalysisBean bean) {
        String startDate = DateUtil.addDays(bean.getEndDate(), -bean.getPeriodDays() + BigInteger.ONE.intValue());
        RfmAnalysisTaskNew taskinfo = new RfmAnalysisTaskNew(bean.getDatasetId(),
                bean.getOutputPath(),
                startDate,
                bean.getEndDate(),
                bean.getDataType().getType(),
                dataSet,
                bean.getRfmAnalysisConfig().getJfsEndPoint(),
                bean.getRfmAnalysisConfig().getAccessKey(),
                bean.getRfmAnalysisConfig().getSecretKey()
        );
        RfmAnalysisConfigInfo config = new RfmAnalysisConfigInfo(bean.getRfmAnalysisConfig().getJfsBucket(),
                bean.getRfmAnalysisConfig().getPurchaseBehaviorInfo(), bean.getRfmAnalysisConfig().getPurchaseDurationDistribution(),
                bean.getRfmAnalysisConfig().getPurchaseFrequencyDistribution(), bean.getRfmAnalysisConfig().getRepurchaseCycleDistribution(),
                bean.getRfmAnalysisConfig().getMonetaryDistribution(), bean.getRfmAnalysisConfig().getPurchaseAmountDistribution(), bean.getRfmAnalysisConfig().getPurchaseChannelDistribution(),
                bean.getRfmAnalysisConfig().getConsumerLayer());
        RfmAnalysisTaskNew[] arrTask = new RfmAnalysisTaskNew[NumberConstant.INT_1];
        arrTask[NumberConstant.INT_0] = taskinfo;
        RfmAnalysis.compute(spark, config, arrTask);
    }

    private Map<String, List<DatasetUserIdConf>> getOneIdConfigInfo(RfmAlalysisBean bean, Boolean isTest) {
        Map<String, List<DatasetUserIdConf>> config = new HashMap<>();
        if (isTest) {
            config.put("oneid_rfm_user_8_9", new ArrayList<DatasetUserIdConf>() {
                {
                    DatasetUserIdConf conf1 = new DatasetUserIdConf();
                    conf1.setUserIdType(8L);
                    conf1.setUserField("user_8");
                    add(conf1);
                    DatasetUserIdConf conf2 = new DatasetUserIdConf();
                    conf2.setUserIdType(9L);
                    conf2.setUserField("user_9");
                    add(conf2);
                }
            });
            config.put("oneid_rfm_user_9_10", new ArrayList<DatasetUserIdConf>() {
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
            config = CommonDealUtils.getSourceOneIdConfig(bean.getRelDataset().getData(), userIdTb);
        }
        return config;
    }

    /**
     * @throws
     * @title dealJoinSql
     * @description 处理每个关联表，以及关联字段
     * @author cdxiongmei
     * @param: recentField 订单日期字段
     * @param: tableConfig 表关联字段，分区字段
     * @param: bean 任务参数
     * @param: tableAlias 表别名
     * @updateTime 2021/8/11 上午9:41
     * @return: java.util.LinkedList<java.lang.String>
     */
    private LinkedList<String> dealOneJoinSql(SparkSession spark, RfmAlalysisBean bean, Map<String, String> tableAlias, Map<String, List<String>> tableFields, boolean isTest) {

        Boolean isOneIdJoin = bean.getRelDataset().getData().stream().filter(datasource -> datasource.isUseOneId()).count() > NumberConstant.INT_0;
        Dataset<Row> oneIdDs = spark.emptyDataFrame();
        if (isOneIdJoin || bean.isUseOneId()) {
            LOGGER.error("查询oneid表");
            oneIdDs = spark.read().table(ConfigProperties.ONEID_DETAIL_TB);
            oneIdDs.cache();
        }
        String mainTable = bean.getUserIdField().split("\\.")[1];
        Boolean isOneIdRet = bean.isUseOneId();
        /**
         * 数据源的oneid配置信息
         */
        Map<String, List<DatasetUserIdConf>> config = new HashMap<String, List<DatasetUserIdConf>>();
        if (isOneIdJoin || isOneIdRet) {
            config = getOneIdConfigInfo(bean, isTest);
        }
        /**
         * 存放子表的表达式
         * 1. 如果使用普通字段关联，则存具体的子表表达式
         * 2. 如果使用oneid关联，则存表别名
         */
        LinkedList<String> tableFilterExp = new LinkedList<String>();
        for (RelDatasetModel sourceInfo : bean.getRelDataset().getData()) {
            String tableName = sourceInfo.getDataTable();
            //如果是最近消费或者订单时间，需要再加一个时间过滤
            boolean isNeedOneIdJoin = sourceInfo.isUseOneId() || (isOneIdRet && tableName.equalsIgnoreCase(mainTable));
            if (isNeedOneIdJoin) {
                /**
                 * 需要进行oneid关联，扩展用户标示字段集合
                 */
                List<String> userIds = SparkUdfUtil.getUserIdsFromOneId(config.get(tableName));
                List<String> labelist = tableFields.computeIfAbsent(tableAlias.get(tableName), k -> new ArrayList<String>());
                labelist.addAll(userIds);
            }
            StringBuilder strChildTbSql = new StringBuilder("SELECT " + StringUtils.join(tableFields.get(tableAlias.get(tableName)), StringConstant.COMMA));
            strChildTbSql.append(filterExpress(bean, tableName));
            String subTableExpress = "";
            if (!isNeedOneIdJoin) {
                subTableExpress = " (" + strChildTbSql.toString() + ") " + tableAlias.get(tableName);
            } else {
                //需要关联oneid
                LOGGER.error("需要进行oneid关联，tablename=" + tableName + ",sql:" + strChildTbSql.toString());
                Dataset<Row> dataOrigin = spark.sql(strChildTbSql.toString());
                String[] columns = dataOrigin.columns();
                Map<String, String> columnsMap = new HashMap<>();
                Map<String, String> renameMap = new HashMap<>();
                Arrays.stream(columns).forEach(col -> {
                    columnsMap.put(col, "max");
                    renameMap.put("max(" + col + ")", col);
                });
                Dataset<Row> nextOrigin = dataOrigin;
                Integer sourceCnt = config.get(tableName).size();
                Dataset<Row> resultData = spark.emptyDataFrame();
                for (int i = 0; i < sourceCnt; i++) {
                    DatasetUserIdConf oneIdConf = config.get(tableName).get(i);
                    //当前参与关联的用户标示字段
                    String curuseridField = "user_" + oneIdConf.getUserIdType();
                    Dataset<Row> curOrigin = nextOrigin.filter(curuseridField + " IS NOT NULL AND " + curuseridField + " !=\"\"")
                            .withColumn("relation_field", functions.concat(functions.col(curuseridField), functions.lit("_" + oneIdConf.getUserIdType())));

                    nextOrigin = nextOrigin.filter(curuseridField + " IS  NULL OR " + curuseridField + " =\"\"");
                    if (resultData.isEmpty()) {
                        resultData = curOrigin;
                    } else if (!curOrigin.isEmpty()) {
                        resultData = resultData.unionAll(curOrigin);
                    }
                }
                //和oneid表进行关联
                String oneidJoinView = tableAlias.get(tableName);
                LOGGER.error("生成临时表：" + oneidJoinView);
                Dataset<Row> dataset = resultData.join(oneIdDs.selectExpr("one_id", "concat(`id`,'_' ,`id_type`) AS relation_field"), "relation_field");
                /**
                 * 使用不同字段关联oneid，可能出现重复用户，对用户进行去重
                 */
                dataset = dataset.groupBy("one_id").agg(columnsMap);
                for (String colName : renameMap.keySet()) {
                    dataset = dataset.withColumnRenamed(colName, renameMap.get(colName));
                }
                if (sourceInfo.isUseOneId()) {
                    dataset = dataset.withColumn("relation_id", functions.col("one_id"));
                }
                if (isOneIdRet && tableName.equalsIgnoreCase(mainTable)) {
                    dataset = dataset.withColumn("user_id", functions.col("one_id"));
                }
                dataset.cache();
                if (isTest) {
                    LOGGER.error("table view:" + oneidJoinView);
                    dataset.show(NumberConstant.INT_10);
                    dataset.printSchema();
                }
                dataset.createOrReplaceTempView(oneidJoinView);
                subTableExpress = oneidJoinView;
            }
            /**
             * 建立多表之间的关联关系
             */
            if (tableName.equalsIgnoreCase(mainTable)) {
                tableFilterExp.addFirst(subTableExpress);
            } else {
                String joinSubSql = "";
                joinSubSql += " ON " + tableAlias.get(mainTable) + ".relation_id=";
                joinSubSql += tableAlias.get(tableName) + ".relation_id";
                tableFilterExp.add(subTableExpress + joinSubSql);
            }
        }
        return tableFilterExp;

    }

    /**
     * 获取最近分析周期的过滤条件
     *
     * @param bean
     * @param currentTb
     * @return eg:dt>= '2021-01-01' and order_dt>='2021-01-01' and order_dt<='2021-03-01'
     */
    private String filterExpress(RfmAlalysisBean bean, String currentTb) {
        RelDatasetModel sourceInfo = bean.getRelDataset().getData().stream().filter(source -> source.getDataTable().equalsIgnoreCase(currentTb)).findFirst().get();

        StringBuilder str = new StringBuilder(" FROM " + currentTb + " WHERE " + sourceInfo.getDateField());
        String recentField = "";
        /**
         * 分区字段过滤
         */
        if (bean.getDataType().getValue().equalsIgnoreCase(RFMDataTypeEnum.deal.getValue())) {
            //分区字段范围
            String dayHis = DateUtil.addDays(bean.getEndDate(), -DT_FILTER_DAYS - bean.getPeriodDays() + BigInteger.ONE.intValue());
            str.append(" >= \"").append(dayHis).append("\"");
            recentField = bean.getOrderDtField();
        } else if (bean.getDataType().getValue() == RFMDataTypeEnum.user.getValue()) {
            //用户数据取统计某一天分区的数据
            str.append(" = \"").append(bean.getEndDate()).append("\"");
            recentField = bean.getRecentyField();
        }
        /**
         * 分析周期数据过滤（交易数据使用下单日期字段，用户数据使用最近下单字段过滤）
         */
        String recentTb = recentField.split("\\.")[1];
        if (currentTb.equalsIgnoreCase(recentTb)) {
            String dayHisOrder = DateUtil.addDays(bean.getEndDate(), -bean.getPeriodDays() + BigInteger.ONE.intValue());
            String fieldName = recentField.split("\\.")[2];
            str.append(" AND ").append(fieldName).append(" >= \"").append(dayHisOrder).append("\"");
            str.append(" AND ").append(fieldName).append(" <= \"").append(bean.getEndDate()).append("\"");
        }
        /**
         * 过滤普通字段关联，但关联字段为空的数据
         */
        if (StringUtils.isNotBlank(sourceInfo.getRelationField()) && !sourceInfo.isUseOneId()) {
            str.append(" AND " + sourceInfo.getRelationField() + " is not null AND " + sourceInfo.getRelationField() + "!=''");
        }
        return str.toString();
    }

    private LinkedList<String> dealJoinSql(RfmAlalysisBean bean, String userIdTable, Map<String, String> tableAlias, Map<String, List<String>> tableFields, Map<String, RelDatasetModel> tableConfig) {
        //分区字段范围
        String recentField = bean.getRecentyField();
        if (bean.getDataType().getValue().equalsIgnoreCase(RFMDataTypeEnum.deal.getValue())) {
            recentField = bean.getOrderDtField();
        }
        String dayHis = DateUtil.addDays(bean.getEndDate(), -DT_FILTER_DAYS - bean.getPeriodDays() + BigInteger.ONE.intValue());
        //存放子表表达式
        LinkedList<String> tableFilterExp = new LinkedList<String>();
        for (String tableName : tableAlias.keySet()) {
            //如果是最近消费或者订单时间，需要再加一个时间过滤
            StringBuilder str = new StringBuilder("(SELECT " + StringUtils.join(tableFields.get(tableAlias.get(tableName)), StringConstant.COMMA) + " FROM ").append(tableName).append(" WHERE ").append(tableConfig.get(tableName).getDateField());
            if (bean.getDataType().getValue() == RFMDataTypeEnum.deal.getValue()) {
                str.append(" >= \"").append(dayHis).append("\"");
            } else if (bean.getDataType().getValue() == RFMDataTypeEnum.user.getValue()) {
                //用户数据取统计某一天分区的数据
                str.append(" = \"").append(bean.getEndDate()).append("\"");
            }
            if (recentField.startsWith(new StringBuilder(tableName).append(StringConstant.DOTMARK).toString())) {
                String dayHisOrder = DateUtil.addDays(bean.getEndDate(), -bean.getPeriodDays() + BigInteger.ONE.intValue());
                String fieldName = recentField.replaceAll(new StringBuilder(tableName).append(StringConstant.DOTMARK).toString(), StringConstant.EMPTY);
                str.append(" AND ").append(fieldName).append(" >= \"").append(dayHisOrder).append("\"");
                str.append(" AND ").append(fieldName).append(" <= \"").append(bean.getEndDate()).append("\"");
            }
            str.append(")").append(tableAlias.get(tableName));
            if (tableName.equalsIgnoreCase(userIdTable)) {
                tableFilterExp.addFirst(str.toString());
                continue;
            }
            str.append(" ON ").append(tableAlias.get(userIdTable)).append(StringConstant.DOTMARK).append(tableConfig.get(userIdTable).getRelationField()).append(" = ");
            str.append(tableAlias.get(tableName)).append(StringConstant.DOTMARK).append(tableConfig.get(tableName).getRelationField());
            tableFilterExp.add(str.toString());
        }
        return tableFilterExp;
    }
    /**
     * 拼接sql
     *
     * @param spark
     * @param bean
     * @return
     */
    public Dataset<Row> buildSql(SparkSession spark, RfmAlalysisBean bean, boolean isTest) {
        // 对应表的别名
        Map<String, String> tableAlias = new HashMap<>(NumberConstant.INT_20);
        List<RelDatasetModel> tableList = bean.getRelDataset().getData();

        //是否使用oneid做结果
        Boolean isOneIdRet = bean.isUseOneId();
        //1. 组装查询sql语句
        //userid 的表需要替换别名(判断是否是oneid类型)
        String mainTable = bean.getUserIdField().split("\\.")[1];
        // 所取字段集合
        Map<String, List<String>> fieldList = new HashMap<String, List<String>>();

        for (int i = BigInteger.ZERO.intValue(); i < tableList.size(); i++) {
            final RelDatasetModel table = tableList.get(i);
            // 表别名处理
            final String tableAliasName = "tbl_" + i;
            String tableName = new StringBuilder(table.getDataTable()).toString();
            tableAlias.computeIfAbsent(tableName, k -> tableAliasName);
            // 使用userid关联，则重命名关联字段
            List<String> labelist = fieldList.computeIfAbsent(tableAliasName, k -> new ArrayList<String>());
            if (!table.isUseOneId() && tableList.size() > 1) {
                labelist.add(table.getRelationField() + " AS relation_id");
            }
            if (!isOneIdRet && tableName.equalsIgnoreCase(mainTable)) {
                labelist.add(bean.getUserIdField().split("\\.")[2] + " AS user_id");
            }
        }
        //4.拼接标签数据集sql的字段
        List<String> finalFields = new ArrayList<String>();
        finalFields.add("CAST(" + tableAlias.get(mainTable) + ".user_id AS STRING) AS user_log_acct");

        //rfm字段
        if (bean.getDataType().getValue() == RFMDataTypeEnum.user.getValue()) {
            //用户数据
            parseFieldAlias(bean.getFrequencyField(), "frequency", fieldList, tableAlias);
            parseFieldAlias(bean.getMontaryField(), "monetary", fieldList, tableAlias);
            parseFieldAlias(bean.getRecentyField(), "latest_dt", fieldList, tableAlias);
            finalFields.add("CAST(frequency AS INT) AS frequency");
            finalFields.add("CAST(monetary AS DOUBLE) AS monetary");
            finalFields.add("0 AS amount");
            finalFields.add("array('other')  AS channel");
            finalFields.add("'1900-01-01' AS first_dt");
            finalFields.add("CAST(latest_dt AS STRING) AS latest_dt");

        } else if (RFMDataTypeEnum.deal.getValue().equals(bean.getDataType().getValue())) {
            //交易数据
            parseFieldAlias(bean.getSaleIdField(), "parent_sale_ord_id", fieldList, tableAlias);
            parseFieldAlias(bean.getMontaryField(), "monetary", fieldList, tableAlias);
            parseFieldAlias(bean.getOrderDtField(), "dt", fieldList, tableAlias);
            finalFields.add("CAST(parent_sale_ord_id AS STRING) AS parent_sale_ord_id");
            finalFields.add("CAST(monetary AS DOUBLE) AS monetary");
            finalFields.add("0 AS amount");
            finalFields.add("'other' AS channel");
            finalFields.add("CAST(dt AS STRING) AS dt");
        }

        //获取每个字表的查询
        LinkedList<String> tableFilterExp = new LinkedList<String>();
        tableFilterExp = dealOneJoinSql(spark, bean, tableAlias, fieldList, isTest);
        //拼接最终sql表达式
        StringBuilder sqlBuilder = new StringBuilder("SELECT " + StringUtils.join(finalFields, ",") + " FROM ");
        String sql = sqlBuilder.append(String.join(" JOIN ", tableFilterExp)).toString();
        LOGGER.error("final sql = {}", sql);
        Dataset<Row> dataset = spark.sql(sql);
        dataset.cache();
        if (dataset.count() == BigInteger.ZERO.intValue()) {
            throw new JobInterruptedException("rfm模型匹配数据为0", "Error: The size of RFM_Dataset is zero!");
        }
        // 格式化数据
        StructType schema = DataTypes.createStructType(userfields);
        Dataset<Row> dsAlalysis = spark.emptyDataFrame();
        if (bean.getDataType().getType() == RFMDataTypeEnum.user.getType()) {
            dsAlalysis = dataset.select("user_log_acct", "frequency", "monetary", "amount", "channel", "first_dt", "latest_dt");
        } else if (bean.getDataType().getType() == RFMDataTypeEnum.deal.getType()) {
            schema = DataTypes.createStructType(dealfields);
            dsAlalysis = dataset.select("user_log_acct", "parent_sale_ord_id", "monetary", "amount", "channel", "dt");
        }
        return spark.createDataFrame(dsAlalysis.toJavaRDD(), schema).filter("user_log_acct is not null and user_log_acct !=''");
    }

    /**
     * @throws
     * @title parseTableName
     * @description 正则表达式解析表名
     * @author cdxiongmei
     * @param: fieldName
     * @updateTime 2021/8/10 下午8:14
     * @return: java.lang.String
     */
    private String parseTableName(String fieldName) {
        Matcher m = DB_TABLENAME_SPLIT.matcher(fieldName);
        String tableName = "";
        if (m.find()) {
            tableName = m.group(BigInteger.ONE.intValue());
            return tableName;
        } else {
            throw new RuntimeException(new StringBuilder(fieldName).append(" alias handler exception").toString());
        }

    }

    /**
     * @throws
     * @title parseFieldAlias
     * @description 使用表别名进行替换
     * @author cdxiongmei
     * @param: fieldName
     * @param: tableAlias
     * @updateTime 2021/8/10 下午8:14
     * @return: java.lang.String
     */
    private String parseFieldAlias(String fieldName, Map<String, String> tableAlias) {
        String tableName = parseTableName(fieldName);
        return fieldName.replaceAll(tableName, tableAlias.get(tableName));

    }

    private void parseFieldAlias(String fieldName, String newFieldName, Map<String, List<String>> tableField, Map<String, String> tableAlias) {
        String fieldPure = fieldName.split("\\.")[2];
        String tableName = fieldName.split("\\.")[1];
        List<String> labelist = tableField.computeIfAbsent(tableAlias.get(tableName), k -> new ArrayList<String>());
        labelist.add(fieldPure + " AS " + newFieldName);
    }

    private void deleteEsData(RfmAlalysisBean bean) {
        SqlCondition condDatasetId = new SqlCondition();
        condDatasetId.setColumnName("analyseId");
        condDatasetId.setParam1(bean.getDatasetId().toString());
        condDatasetId.setCompareOpType(ComparisonOperatorsEnum.EQUALITY);
        condDatasetId.setLogicalOperator(LogicalOperatorEnum.and);
        //endate
        SqlCondition condEndDate = new SqlCondition();
        condEndDate.setColumnName("endDate");
        condEndDate.setParam1(bean.getDatasetId().toString());
        condEndDate.setCompareOpType(ComparisonOperatorsEnum.EQUALITY);
        condEndDate.setLogicalOperator(LogicalOperatorEnum.and);
        List<SqlCondition> conds = new ArrayList<SqlCondition>() {
            {
                add(condDatasetId);
                add(condEndDate);
            }
        };
        JSONObject dslJson = EsUtil.queryDslJSON(conds);
        EsCluster esCluster = new EsCluster();
        com.jd.easy.audience.task.commonbean.bean.RfmAnalysisConfig esConfig = bean.getRfmAnalysisConfig();
        esCluster.setEsNodes(esConfig.getEsNode());
        esCluster.setEsPort(esConfig.getEsPort());
        esCluster.setUsername(esConfig.getEsUser());
        esCluster.setPassword(esConfig.getEsPassword());
        esCluster.setEsIndex(esConfig.getPurchaseAmountDistribution());
        EsUtil.deleteEsData(esCluster, dslJson);
        esCluster.setEsIndex(esConfig.getPurchaseChannelDistribution());
        EsUtil.deleteEsData(esCluster, dslJson);
        esCluster.setEsIndex(esConfig.getPurchaseBehaviorInfo());
        EsUtil.deleteEsData(esCluster, dslJson);
        esCluster.setEsIndex(esConfig.getPurchaseDurationDistribution());
        EsUtil.deleteEsData(esCluster, dslJson);
        esCluster.setEsIndex(esConfig.getPurchaseFrequencyDistribution());
        EsUtil.deleteEsData(esCluster, dslJson);
        esCluster.setEsIndex(esConfig.getRepurchaseCycleDistribution());
        EsUtil.deleteEsData(esCluster, dslJson);
        esCluster.setEsIndex(esConfig.getMonetaryDistribution());
        EsUtil.deleteEsData(esCluster, dslJson);
        esCluster.setEsIndex(esConfig.getConsumerLayer());
        EsUtil.deleteEsData(esCluster, dslJson);
        LOGGER.info("删除数据成功dslJson=" + dslJson.toJSONString());
    }
}
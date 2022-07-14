package com.jd.easy.audience.task.dataintegration.step;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.jd.easy.audience.common.constant.NumberConstant;
import com.jd.easy.audience.common.constant.SplitDataSourceEnum;
import com.jd.easy.audience.common.constant.SplitDataTypeEnum;
import com.jd.easy.audience.common.constant.StringConstant;
import com.jd.easy.audience.common.exception.JobInterruptedException;
import com.jd.easy.audience.common.util.JsonUtil;
import com.jd.easy.audience.task.commonbean.bean.SynData2PublicBean;
import com.jd.easy.audience.task.dataintegration.property.ConfigProperties;
import com.jd.easy.audience.task.dataintegration.util.DbManagerService;
import com.jd.easy.audience.task.dataintegration.util.LoadDataJnosRelInfo;
import com.jd.easy.audience.task.dataintegration.util.SparkUtil;
import com.jd.easy.audience.task.driven.step.StepCommon;
import com.jd.easy.audience.task.driven.step.StepCommonBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalog.Column;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author cdxiongmei
 * @title SynData2PublicStep
 * @description 根据指定的源表进行数据的同步
 * @updateTime 2021/11/14 下午3:05
 * @throws
 */
public class SynData2PublicStep extends StepCommon<Map<String, Object>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SynData2PublicStep.class);

    @Override
    public void validate() {
        super.validate();
        SynData2PublicBean bean = (SynData2PublicBean) getStepBean();
        LOGGER.info("bean info:" + JsonUtil.serialize(bean));

        if (StringUtils.isAnyBlank(bean.getSourceDbName(), bean.getSourceTableName(), bean.getSubTableNamePrefix())) {
            LOGGER.error("同步数据必须要指定源表和目标表前缀");
            throw new JobInterruptedException("同步数据必须要指定源表和目标表前缀", "split data must have source table and target table");
        }
        if (StringUtils.isBlank(bean.getTenantFieldSource()) || bean.getTenantFieldSource().split(",").length > NumberConstant.INT_2) {
            throw new JobInterruptedException("tenantRelFiled length greater than 2");
        }
        if (bean.getDataType().isDesPartitioned() && StringUtils.isBlank(bean.getDtFieldSource())) {
            throw new JobInterruptedException("目标表为分区表，则必须配置时间分区字段");
        }
        boolean hasDt = StringUtils.isNotBlank(bean.getDtFieldSource());
        boolean hasFieldsFilter = StringUtils.isNotBlank(bean.getFieldFilter());
        boolean hasdtIn = true;
        if (hasDt && hasFieldsFilter) {
            hasdtIn = Arrays.asList(bean.getFieldFilter().split(",")).contains(bean.getDtFieldSource());
        }
        if (!hasdtIn) {
            throw new JobInterruptedException("目标表为分区表，则必须配置时间分区字段");
        }
    }

    @Override
    public Map<String, Object> run(Map<String, StepCommonBean> map) throws Exception {
        //1.spark初始化
        SynData2PublicBean bean = (SynData2PublicBean) getStepBean();
        SparkSession sparkSession = SparkUtil.buildMergeFileSession("LoadDataStep2HiveDatabase_" + bean.getSourceTableName());
        String sourceTable = bean.getSourceDbName() + StringConstant.DOTMARK + bean.getSourceTableName();
        SplitDataTypeEnum dataType = bean.getDataType();
        //2. 拆分时间范围
        int beforeDays = BigInteger.ONE.intValue();
        if (bean.getDateBefore() >= BigInteger.ZERO.intValue()) {
            beforeDays = bean.getDateBefore();
        }
        Date date = new Date();
        String buffaloTime = System.getenv(StringConstant.BUFFALO_ENV_NTIME);
        if (StringUtils.isNotBlank(buffaloTime)) {
            LOGGER.info("BUFFALO_ENV_NTIME ={}", buffaloTime);
            DateTime jobTime = DateTime.parse(buffaloTime, DateTimeFormat.forPattern(StringConstant.DATEFORMAT_YYYYMMDDHHMMSS));
            date = jobTime.toDate();
        }
        String filterDate = getDateStrBeforeNDays(date, beforeDays);
        LOGGER.info("ready to splitdata,filterDate=" + filterDate);

        //校验公共库中的表是否存在
        String synTableName = ConfigProperties.PUBLIC_DB_NAME() + StringConstant.DOTMARK + bean.getDesTableName();
        if (bean.getIsRebuilt() && sparkSession.catalog().tableExists(synTableName)) {
            String deleteTb = "DROP TABLE " + synTableName;
            LOGGER.info("ae_public table is drop, sql:" + deleteTb);
            sparkSession.sql(deleteTb);
        }

        //分区字段
        List<String> partitionedFields = new ArrayList<String>() {
//            {
//                add("bdp_prod_account");
//            }
        };
        if (dataType.isDesPartitioned()) {
            partitionedFields.add("dt");
        }
        if (bean.getDataSource().getValue().equalsIgnoreCase(SplitDataSourceEnum.CRM.getValue())) {
            //如果是crm的表，需要加
            partitionedFields.add("account_id");
        } else {
            partitionedFields.addAll(Arrays.asList(bean.getTenantFieldSource().split(StringConstant.COMMA)));
        }
        if (StringUtils.isNotBlank(bean.getSplitFieldEx())) {
            partitionedFields.addAll(Arrays.asList(bean.getSplitFieldEx().split(StringConstant.COMMA)));
        }
        LOGGER.info("partitionedFields is new:" + partitionedFields.toString());
        List<String> selectedFields = new ArrayList<>();
        if (StringUtils.isBlank(bean.getFieldFilter())) {
            selectedFields = sparkSession.catalog().listColumns(sourceTable).select("name").as(Encoders.STRING()).collectAsList();
        } else {
            selectedFields.addAll(Arrays.asList(bean.getFieldFilter().split(StringConstant.COMMA)));
        }
        String subTableField = StringConstant.EMPTY;
        if (!sparkSession.catalog().tableExists(synTableName)) {
            //租户中的标签表不存在
            LOGGER.info("table is new:" + synTableName);
            subTableField = createTableSql(selectedFields, sparkSession, bean, partitionedFields);
        } else {
            LOGGER.info("table is exsit:" + synTableName);
            subTableField = StringUtils.join(sparkSession.catalog().listColumns(synTableName).collectAsList().stream().map(column -> column.name()).collect(Collectors.toList()), ",");
        }
        //4. 过滤筛选字段
        StringBuilder sqlSource = new StringBuilder("SELECT * FROM " + sourceTable);

        //5. 判断源表的分区类型和是否为全量来决定取数规则
        if (dataType.isSourcePartitioned() && !dataType.isAllUpdate()) {
            //5.1 源表是分区表且部分更新
            sqlSource.append(" WHERE " + bean.getDtFieldSource() + "=\'" + filterDate + "\'");
        }
        if (StringUtils.isNotBlank(bean.getFilterExpress())) {
            //5.2 额外的过滤条件
            String conbineFlag = " AND ";
            if (!sqlSource.toString().contains("WHERE")) {
                conbineFlag = " WHERE ";
            }
            sqlSource.append(conbineFlag + bean.getFilterExpress());
        }
        LOGGER.info("sqlSource:" + sqlSource.toString());
        //6. 取出源表的数据
        Dataset<Row> sourceHiveData = sparkSession.sql(sqlSource.toString());
        sourceHiveData.cache();
        long sourceCnt = sourceHiveData.count();
        LOGGER.info("sourceHiveData:" + sourceCnt);
        if (sourceCnt <= BigInteger.ZERO.intValue()) {
            LOGGER.info("没有数据，无需处理");
            return new HashMap<String, Object>();
        }
        //获取主子账号关系
        Map<String, LoadDataJnosRelInfo> dbRelInfo = getTenantRelInfo(bean.getDataSource(), sourceHiveData, bean.getTenantFieldSource(), bean.getSplitFieldEx());
        Set<String> cdpUsers = dbRelInfo.keySet();
        if (cdpUsers.isEmpty()) {
            return new HashMap<String, Object>();
        }
        //注册获取租户信息的udf函数
//        sparkSession.udf().register("getDbName",
//                ((UDF1<String, String>) accountKey -> {
//                    if (dbRelInfo.containsKey(accountKey)) {
//                        return dbRelInfo.get(accountKey).getDbName();
//                    }
//                    return StringConstant.EMPTY;
//                }), DataTypes.StringType);
        sparkSession.udf().register("getAccountInfo",
                ((UDF1<String, String>) accountKey -> {
                    if (dbRelInfo.containsKey(accountKey)) {
                        return dbRelInfo.get(accountKey).getAccountId() + StringConstant.UNDERLINE + dbRelInfo.get(accountKey).getMainAccountName() + StringConstant.UNDERLINE + dbRelInfo.get(accountKey).getSubAccountName();
                    }
                    return StringConstant.EMPTY;
                }), DataTypes.StringType);
        sparkSession.udf().register("getAccountId",
                ((UDF1<String, String>) accountKey -> {
                    if (dbRelInfo.containsKey(accountKey)) {
                        return dbRelInfo.get(accountKey).getAccountId().toString();
                    }
                    return StringConstant.EMPTY;
                }), DataTypes.StringType);
//        sparkSession.udf().register("getbdpAccountInfo",
//                ((UDF1<String, String>) accountKey -> {
//                    if (dbRelInfo.containsKey(accountKey)) {
//                        return dbRelInfo.get(accountKey).getBdpProdAccount();
//                    }
//                    return StringConstant.EMPTY;
//                }), DataTypes.StringType);

        String[] tenantFields = bean.getTenantFieldSource().split(StringConstant.COMMA);
//        String subTableName = "sub_table_name";
        if (tenantFields.length == NumberConstant.INT_2) {
            String accountKey = "account_info_key";
            sourceHiveData = sourceHiveData.withColumn(accountKey, functions.concat(functions.col(tenantFields[NumberConstant.INT_0]), functions.lit(StringConstant.COMMA), functions.col(tenantFields[NumberConstant.INT_1])));
            sourceHiveData = sourceHiveData.filter(accountKey + " in (\"" + StringUtils.join(cdpUsers, "\",\"") + "\")");
//            sourceHiveData = sourceHiveData.withColumn("db_name", functions.callUDF("getDbName", functions.col(accountKey).cast(DataTypes.StringType)));
//            sourceHiveData = sourceHiveData.withColumn(subTableName, functions.concat(functions.col("db_name").cast(DataTypes.StringType), functions.lit("." + bean.getSubTableNamePrefix()), functions.col(tenantFields[0]), functions.lit("_"), functions.col(tenantFields[1])));
            sourceHiveData = sourceHiveData.withColumn("account_info", functions.callUDF("getAccountInfo", functions.col(accountKey).cast(DataTypes.StringType)));
//            sourceHiveData = sourceHiveData.withColumn("bdp_prod_account", functions.callUDF("getbdpAccountInfo", functions.col(accountKey).cast(DataTypes.StringType)));
        } else {
            sourceHiveData = sourceHiveData.filter(tenantFields[NumberConstant.INT_0] + " in (\"" + StringUtils.join(cdpUsers, "\",\"") + "\")");
//            sourceHiveData = sourceHiveData.withColumn("db_name", functions.callUDF("getDbName", functions.col(tenantFields[0]).cast(DataTypes.StringType)));
//            sourceHiveData = sourceHiveData.withColumn(subTableName, functions.concat(functions.col("db_name").cast(DataTypes.StringType), functions.lit("." + bean.getSubTableNamePrefix()), functions.col(tenantFields[0])));
            sourceHiveData = sourceHiveData.withColumn("account_info", functions.callUDF("getAccountInfo", functions.col(bean.getTenantFieldSource()).cast(DataTypes.StringType)));
//            sourceHiveData = sourceHiveData.withColumn("bdp_prod_account", functions.callUDF("getbdpAccountInfo", functions.col(bean.getTenantFieldSource()).cast(DataTypes.StringType)));
        }
        //进行二级拆分
//        if (StringUtils.isNotBlank(bean.getSplitFieldEx())) {
//            for (String filterField : bean.getSplitFieldEx().split(StringConstant.COMMA)) {
//                LOGGER.info("filterField:" + filterField);
//                sourceHiveData = sourceHiveData.withColumn(subTableName + "_new", functions.concat(functions.col(subTableName), functions.lit(StringConstant.UNDERLINE), functions.col(filterField))).drop(subTableName).withColumnRenamed(subTableName + "_new", subTableName);
//            }
//        }
        sourceHiveData = sourceHiveData.withColumn("source_type_ex", functions.lit(bean.getSourceTypeEx().getType()).cast(DataTypes.StringType));
        sourceHiveData = sourceHiveData.withColumn("table_comment", functions.lit(bean.getTableComment()));
        //源表时间字段处理
        if (!dataType.isDesPartitioned() && StringUtils.isNotBlank(bean.getDtFieldSource())) {
            //目标表不是分区表，筛选数据删除源表的分区字段
            sourceHiveData = sourceHiveData.drop(bean.getDtFieldSource());
        } else if (StringUtils.isNotBlank(bean.getDtFieldSource())) {
            //目标表是分区表，还需要把分区字段调整到最后一列
            String fieldTemp = "field_" + System.currentTimeMillis();
            LOGGER.info("temp field name:" + fieldTemp);
            sourceHiveData = sourceHiveData.withColumn(fieldTemp, new org.apache.spark.sql.Column(bean.getDtFieldSource())).drop(bean.getDtFieldSource()).withColumnRenamed(fieldTemp, "dt");
        } else if (StringUtils.isBlank(bean.getDtFieldSource()) && dataType.isDesPartitioned()) {
            //将非分区表加工成分区表
            sourceHiveData = sourceHiveData.withColumn("dt", org.apache.spark.sql.functions.lit(filterDate));
        }
        if (bean.getDataSource().getValue().equalsIgnoreCase(SplitDataSourceEnum.CRM.getValue())) {
            //如果是crm的表，需要加account字段
            sourceHiveData = sourceHiveData.withColumn("account_id", functions.callUDF("getAccountId", functions.col(bean.getTenantFieldSource()).cast(DataTypes.StringType)));

        }
        LOGGER.info("sourcedata schema:" + bean.getSourceTableName());
        sourceHiveData.printSchema();
        sourceHiveData.show(NumberConstant.INT_10);

        String viewNameTemp = "tempViewTable_" + bean.getSourceTableName();
        sourceHiveData.createOrReplaceTempView(viewNameTemp);
        String dataViewName = bean.getStepName() + StringConstant.UNDERLINE + bean.getSourceTableName();
        String sqlSelected = "SELECT " + subTableField + " FROM " + viewNameTemp;
        LOGGER.info("insert sql:" + sqlSelected);
        sparkSession.sql(sqlSelected).createOrReplaceTempView(dataViewName);
        //数据同步工作
        writeDataToHive(dataViewName, synTableName, sparkSession, partitionedFields, dataType);
        return new HashMap<String, Object>();
    }

    /**
     * 应该返回非分区字段，且字段顺序用来和目标表的建表字段保持一致
     *
     * @param selectedFieldsList
     * @param sparkSession
     * @param bean
     * @param partitionedFields
     * @return
     * @throws AnalysisException
     */
    private String createTableSql(List<String> selectedFieldsList, SparkSession sparkSession, SynData2PublicBean bean, List<String> partitionedFields) throws AnalysisException {
        String desName = ConfigProperties.PUBLIC_DB_NAME() + StringConstant.DOTMARK + bean.getDesTableName();
        String sourceTable = bean.getSourceDbName() + StringConstant.DOTMARK + bean.getSourceTableName();
        StringBuilder createSql = new StringBuilder("CREATE TABLE " + desName + "(");
        StringBuilder finalCreateSql = new StringBuilder();
        List<Column> sourceCols = sparkSession.catalog().listColumns(sourceTable).collectAsList();
        List<String> desTableFields = new ArrayList<>();
        List<String> partitionedFieldsDes = new ArrayList<String>();
        partitionedFieldsDes.addAll(partitionedFields.stream().map(s -> s + " STRING").collect(Collectors.toList()));
        for (Column field : sourceCols) {
            if (!selectedFieldsList.contains(field.name()) || field.name().equalsIgnoreCase(bean.getDtFieldSource()) || partitionedFields.contains(field.name())) {
                //没在过滤配置中的字段或者分区字段
                continue;
            } else if (selectedFieldsList.contains(field.name()) && (field.dataType().toLowerCase().contains("array")
                    || field.dataType().toLowerCase().contains("struct")
                    || field.dataType().toLowerCase().contains("map"))) {
                //删除复合类型字段
                continue;
            }
            //添加到子表建表中
            desTableFields.add(field.name());
            finalCreateSql.append(field.name() + StringConstant.SPACE + field.dataType() + " COMMENT \'" + field.description() + "\',");
        }
        //新增标示字段
//        finalCreateSql.append("sub_table_name STRING COMMENT \'子表表名\',");
        finalCreateSql.append("account_info STRING COMMENT \'账号信息\',");
        finalCreateSql.append("source_type_ex STRING COMMENT \'数据来源说明\',");
        finalCreateSql.append("table_comment STRING COMMENT \'表说明\') PARTITIONED BY(");
//        desTableFields.add("sub_table_name");
        desTableFields.add("account_info");
        desTableFields.add("source_type_ex");
        desTableFields.add("table_comment");
        finalCreateSql.append(StringUtils.join(partitionedFieldsDes, StringConstant.COMMA)).append(") STORED AS ORC tblproperties('orc.compress'='SNAPPY')");
        desTableFields.addAll(partitionedFieldsDes);
        createSql.append(finalCreateSql);
        LOGGER.info("create table sql :" + createSql.toString());
        sparkSession.sql(createSql.toString());
        return StringUtils.join(desTableFields, StringConstant.COMMA);
    }

    /**
     * @throws
     * @title writeDataToHive
     * @description 将拆分后的数据写入对应的hive表
     * @author cdxiongmei
     * @param: viewName
     * @param: tableName
     * @param: sparkSession
     * @param: dtField
     * @param: dataType
     * @updateTime 2021/8/9 下午7:55
     * @return: boolean
     */
    private boolean writeDataToHive(String viewName, String tableName, SparkSession sparkSession, List<String> partitionedFields, SplitDataTypeEnum dataType) {
        LOGGER.info("tableName={}, partitionedFields={},dataType={},viewName={}", tableName, partitionedFields.toString(), dataType, viewName);

        if (!sparkSession.catalog().tableExists(tableName)) {
            LOGGER.error("hive table " + tableName + " is not exists");
            throw new JobInterruptedException(tableName + "不存在", "hive table " + tableName + " is not exists");
        }
        try {
            LOGGER.info("data type=" + dataType.name());
            StringBuilder sql = new StringBuilder("INSERT OVERWRITE TABLE " + tableName);
            if (dataType.isDesPartitioned()) {
                sql.append(" PARTITION(" + StringUtils.join(partitionedFields, StringConstant.COMMA) + ")");
            }
            sql.append("\nSELECT * \nFROM\n" + viewName);
            LOGGER.info("insert sql:" + sql.toString());
            sparkSession.sql(sql.toString());

        } catch (Exception e) {
            LOGGER.error("writeDataToHive exception!", e);
            return false;
        }
        return true;
    }

    /**
     * 获取账号字段和库表之间关系（支持主子两种类型）
     *
     * @param dataSource
     * @param sourceData
     * @param tenantFields
     * @return
     * @throws SQLException
     * @throws IOException
     */
    private Map<String, LoadDataJnosRelInfo> getTenantRelInfo(SplitDataSourceEnum dataSource, Dataset<Row> sourceData, String tenantFields, String fil) throws SQLException, IOException {
        LOGGER.info("getTenantRelInfo dataSource=" + dataSource.getValue());
        Map<String, LoadDataJnosRelInfo> tenantRelInfo = new HashMap<String, LoadDataJnosRelInfo>();
        if (dataSource.getValue().equalsIgnoreCase(SplitDataSourceEnum.CRM.getValue())) {
            String querySql = "select\n" +
                    "  account_id,\n" +
                    "  main_account_name,\n" +
                    "  crm_name,\n" +
                    "  db_id,\n" +
                    "  db_name,\n" +
                    "  bdp_prod_account\n" +
                    "from\n" +
                    "  (\n" +
                    "    select\n" +
                    "      account_id," +
                    "      id as db_id,\n" +
                    "      database_name as db_name,\n" +
                    "      main_account_name,\n" +
                    "      bdp_prod_account\n" +
                    "    from\n" +
                    "      ea_database\n" +
                    "    where\n" +
                    "      yn = 1\n" +
                    "  ) db_tb\n" +
                    "  join (\n" +
                    "    select\n" +
                    "      crm_name,\n" +
                    "      jnos_name\n" +
                    "    from\n" +
                    "      ea_crm_jnos_relation\n" +
                    "    where\n" +
                    "      yn = 1\n" +
                    "  ) rel_tb on db_tb.main_account_name = rel_tb.jnos_name;";
            Connection db2 = DbManagerService.getConn(); //创建DBHelper对象
            Statement stmt = DbManagerService.stmt(db2);
            ResultSet retInfos = DbManagerService.executeQuery(stmt, querySql); //执行语句，得到结果集
            while (retInfos.next()) {
                LOGGER.info("accountId={},mainAccountName={},crm_name={}, dbid={}, dbname={}", retInfos.getLong("account_id"), retInfos.getString("main_account_name"), retInfos.getString("crm_name"), retInfos.getLong("db_id"), retInfos.getString("db_name"));
                //CRM中tenantcode为门店账号id，tenantname为jnos主账号名称
                tenantRelInfo.put(retInfos.getString("crm_name"),
                        new LoadDataJnosRelInfo(retInfos.getLong("account_id"),
                                retInfos.getString("main_account_name"),
                                retInfos.getString("crm_name"),
                                retInfos.getString("main_account_name"),
                                -1L));
            }
            LOGGER.info("tenantRelInfo:" + tenantRelInfo.toString());
            DbManagerService.close(db2, stmt, null, retInfos);
        } else if (dataSource.getValue().equalsIgnoreCase(SplitDataSourceEnum.ACCOUNTID.getValue())) {
            String querySql =
                    "    select\n" +
                            "      account_id," +
                            "      main_account_name,\n" +
                            "      id as db_id,\n" +
                            "      database_name as db_name,\n" +
                            "      bdp_prod_account\n" +
                            "    from\n" +
                            "      ea_database\n" +
                            "    where\n" +
                            "      yn = 1\n";
            Connection db2 = DbManagerService.getConn(); //创建DBHelper对象
            Statement stmt = DbManagerService.stmt(db2);
            ResultSet retInfos = DbManagerService.executeQuery(stmt, querySql); //执行语句，得到结果集
            while (retInfos.next()) {
                LOGGER.info("accountId={},mainAccountName={}, dbid={}, dbname={}", retInfos.getLong("account_id"), retInfos.getString("main_account_name"), retInfos.getLong("db_id"), retInfos.getString("db_name"));
                tenantRelInfo.put(retInfos.getString("account_id"), new LoadDataJnosRelInfo(
                        retInfos.getLong("account_id"),
                        retInfos.getString("main_account_name"),
                        retInfos.getString("account_id"),
                        retInfos.getString("main_account_name"),
                        -1L

                ));
            }
        } else if (dataSource.getValue().equalsIgnoreCase(SplitDataSourceEnum.CA.getValue())) {
            //ca数据拆分则为uid维度
            List<Row> tenantRows = null;
            if (tenantFields.split(StringConstant.COMMA).length == NumberConstant.INT_1) {
                tenantRows = sourceData.select(tenantFields).distinct().collectAsList();
            } else if (tenantFields.split(StringConstant.COMMA).length == NumberConstant.INT_2) {
                tenantRows = sourceData.select(tenantFields.split(StringConstant.COMMA)[0].trim(), tenantFields.split(StringConstant.COMMA)[1].trim()).distinct().collectAsList();
            } else {
                throw new JobInterruptedException("tenantRelFiled length greater than 2");
            }
            List<JSON> tenantIds = new ArrayList<>();
            tenantRows.forEach(row -> {
                LOGGER.info("tenantFields=" + tenantFields + ",row=" + row.toString());
                JSONObject tenant = new JSONObject();
                if (tenantFields.split(StringConstant.COMMA).length == NumberConstant.INT_2) {
                    tenant.put("accountId", row.get(NumberConstant.INT_0).toString());
                    tenant.put("uid", row.get(NumberConstant.INT_1).toString());
                } else if (tenantFields.split(StringConstant.COMMA).length == NumberConstant.INT_1) {
                    tenant.put("uid", row.get(NumberConstant.INT_0).toString());
                }
                tenantIds.add(tenant);
            });
            LOGGER.info("tenantids:" + tenantIds.toString());

            String urlStr = ConfigProperties.SERVICE_URI() + "/ea/api/cdp/accountInfo/list";
            LOGGER.info("ca url:" + urlStr);
            HttpClient client = new DefaultHttpClient();
            HttpResponse response = null;
            HttpPost post = new HttpPost(urlStr);
            post.setHeader("Content-Type", "application/json");
            post.setEntity(new StringEntity(JSONObject.toJSONString(tenantIds)));
            response = client.execute(post);
            String ret = EntityUtils.toString(response.getEntity(), "UTF-8");
            LOGGER.info(ret);
            JSONObject jsonObject = JSON.parseObject(ret);
            if ("0".equals(jsonObject.getString("status"))) {
                LOGGER.info("成功，系统处理正常");
                JSONArray result = jsonObject.getJSONArray("result");
                result.stream().forEach(ele -> {
                    JSONObject eleJson = JSONObject.parseObject(ele.toString());
                    LOGGER.info("accountInfo:" + ele.toString());
                    if (tenantFields.split(StringConstant.COMMA).length == NumberConstant.INT_1) {
                        //根据uid
                        tenantRelInfo.put(eleJson.getString("uid"), new LoadDataJnosRelInfo(
                                        eleJson.getLong("accountId"),
                                        eleJson.getString("mainAccountName"),
                                        eleJson.getString("uid"),
                                        eleJson.getString("name"),
                                        -1L
                                )
                        );
                    } else if (tenantFields.split(StringConstant.COMMA).length == NumberConstant.INT_2) {
                        //根据account_id + uid
                        tenantRelInfo.put(eleJson.getLong("accountId") + "," + eleJson.getString("uid"), new LoadDataJnosRelInfo(
                                        eleJson.getLong("accountId"),
                                        eleJson.getString("mainAccountName"),
                                        eleJson.getString("uid"),
                                        eleJson.getString("name"),
                                        -1L
                                )
                        );
                    }

                });
            }
            LOGGER.info("tenantRel:" + tenantRelInfo.toString());
        }
        return tenantRelInfo;
    }

    private String getDateStrBeforeNDays(Date date, int interval) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DATE, -interval);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormat.format(cal.getTime());
    }

}

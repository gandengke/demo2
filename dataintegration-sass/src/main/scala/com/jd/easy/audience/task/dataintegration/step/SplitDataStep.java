package com.jd.easy.audience.task.dataintegration.step;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.jd.easy.audience.common.constant.NumberConstant;
import com.jd.easy.audience.common.constant.SplitDataSourceEnum;
import com.jd.easy.audience.common.constant.SplitDataTypeEnum;
import com.jd.easy.audience.common.exception.JobInterruptedException;
import com.jd.easy.audience.common.util.JsonUtil;
import com.jd.easy.audience.task.commonbean.bean.SplitDataBean;
import com.jd.easy.audience.task.dataintegration.property.ConfigProperties;
import com.jd.easy.audience.task.dataintegration.util.*;
import com.jd.easy.audience.task.driven.step.StepCommon;
import com.jd.easy.audience.task.driven.step.StepCommonBean;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Column;
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

/**
 * @author cdxiongmei
 * @title SplitDataForCRM
 * @description 对数据进行按租户拆分
 * @updateTime 2021/7/14 下午3:05
 * @throws
 */
@Deprecated
public class SplitDataStep extends StepCommon<Map<String, Object>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SplitDataStep.class);

    @Override
    public void validate() {
        super.validate();
        SplitDataBean bean = (SplitDataBean) getStepBean();
        LOGGER.info("bean info:" + JsonUtil.serialize(bean));

        if (StringUtils.isBlank(bean.getTenantFieldSource()) && bean.getTenantFieldSource().split(",").length > NumberConstant.INT_2) {
            LOGGER.error("拆分数据必须要指定租户字段");
            throw new JobInterruptedException("拆分数据必须要指定租户字段", "split data must have tenant field");
        }
        if (StringUtils.isBlank(bean.getSourceTable()) || StringUtils.isBlank(bean.getDesTablePre())) {
            LOGGER.error("拆分数据必须要指定源表和目标表前缀");
            throw new JobInterruptedException("拆分数据必须要指定源表和目标表前缀", "split data must have source table and target table");
        }
    }

    @Override
    public Map<String, Object> run(Map<String, StepCommonBean> map) throws Exception {
        //1.取出租户和hive数据库的关联关系
        SplitDataBean bean = (SplitDataBean) getStepBean();
        SparkSession sparkSession = SparkUtil.buildMergeFileSession("SplitData_" + bean.getSourceTable());
        String sourceTable = bean.getSourceDbName() + "." + bean.getSourceTable();

        Map<String, Object> ret = new HashMap<String, Object>();
        //拆分场景
        SplitDataTypeEnum dataType = bean.getDataType();
        int beforeDays = BigInteger.ONE.intValue();
        if (bean.getDateBefore() >= BigInteger.ZERO.intValue()) {
            beforeDays = bean.getDateBefore();
        }
        Date date = new Date();
        String buffaloTime = System.getenv("BUFFALO_ENV_NTIME");
        if (StringUtils.isNotBlank(buffaloTime)) {
            LOGGER.info("BUFFALO_ENV_NTIME ={}", buffaloTime);
            DateTime jobTime = DateTime.parse(buffaloTime, DateTimeFormat.forPattern("yyyyMMddHHmmss"));
            date = jobTime.toDate();
        }
        String statDate = getDateStrBeforeNDays(date, beforeDays);

        LOGGER.info("ready to splitdata,dataType= " + dataType + ",statDate=" + statDate + ",tablename=" + sourceTable + ",desTablePre=" + bean.getDesTablePre());
        //拆分后字表的字段加工
        StringBuilder finalCreateSql = new StringBuilder();
        List<Column> sourceCols = sparkSession.catalog().listColumns(sourceTable).collectAsList();
        List<String> fieldFilterArr = new ArrayList<>();
        if (StringUtils.isNotBlank(bean.getFieldFilter())) {
            fieldFilterArr = Arrays.asList(bean.getFieldFilter().split(","));
        }
        LOGGER.info("fieldFilter:" + fieldFilterArr.toString());
        List<String> fieldSelected = new ArrayList<>();
        for (Column field : sourceCols) {
            if (field.dataType().toLowerCase().contains("array")
                    || field.dataType().toLowerCase().contains("struct")
                    || field.dataType().toLowerCase().contains("map")) {
                continue;
            }
            if ((!fieldFilterArr.isEmpty() && !fieldFilterArr.contains(field.name())) || field.name().equalsIgnoreCase(bean.getDtFieldSource())) {
                //没在过滤配置中的字段或者分区字段
                continue;
            }
            finalCreateSql.append(field.name() + " " + field.dataType() + " COMMENT \'" + field.description() + "\',");
            fieldSelected.add(field.name());
        }
        finalCreateSql = new StringBuilder(finalCreateSql.substring(0, finalCreateSql.length() - 1));

        if (!dataType.isDesPartitioned()) {
            finalCreateSql.append(")COMMENT \"" + bean.getTableComment() + "\" STORED AS ORC tblproperties('orc.compress'='SNAPPY')");
        } else {
            finalCreateSql.append(")COMMENT \"" + bean.getTableComment() + "\" PARTITIONED BY (dt string comment '分区字段') STORED AS ORC tblproperties('orc.compress'='SNAPPY')");
            fieldSelected.add(bean.getDtFieldSource() + " AS dt");
        }
        LOGGER.info(finalCreateSql.toString());

        //源数据过滤
        StringBuilder sqlSource = new StringBuilder("SELECT " + StringUtils.join(fieldSelected, ",") + " FROM " + sourceTable);
        if (dataType.isSourcePartitioned() && !dataType.isAllUpdate()) {
            //2.1源表是分区表且部分更新
            sqlSource.append(" WHERE " + bean.getDtFieldSource() + "=\'" + statDate + "\'");
        }
        if (StringUtils.isNotBlank(bean.getFilterExpress())) {
            String conbineFlag = " AND ";
            if (!sqlSource.toString().contains("WHERE")) {
                conbineFlag = " WHERE ";
            }
            sqlSource.append(conbineFlag + bean.getFilterExpress());
        }
        LOGGER.info("sqlSource:" + sqlSource.toString());
        //取出源表的数据
        Dataset<Row> sourceData = sparkSession.sql(sqlSource.toString());
        long sourceCnt = sourceData.count();
        LOGGER.info("sourceCnt:" + sourceCnt);
        if (sourceCnt <= BigInteger.ZERO.intValue()) {
            LOGGER.info("没有数据，无需处理");
            return new HashMap<String, Object>();
        }
        //源表时间字段处理
        sourceData.cache();
        if (StringUtils.isBlank(bean.getDtFieldSource()) && dataType.isDesPartitioned()) {
            //将非分区表加工成分区表
            sourceData = sourceData.withColumn("dt", org.apache.spark.sql.functions.lit(statDate));
        }
        LOGGER.info("sourcedata schema:" + bean.getSourceTable());
        sourceData.printSchema();
        //获取主子账号关系
        List<JnosRelInfo> dbRelInfo = getTenantRelInfo(bean.getDataSource(), sourceData, bean.getTenantFieldSource());
        for (JnosRelInfo dbRel : dbRelInfo) {
            //tenantId需要同时支持主子账号，因此也需要将租户标识追加到目标表的后面
            LOGGER.info("tenant_id={}, dbname={}", dbRel.getFilterCode(), dbRel.getDbName());
            String tenantId = dbRel.getFilterCode();
            Dataset<Row> tenantLabelData = sparkSession.emptyDataFrame();
            String[] fields = bean.getTenantFieldSource().split(",");
            String tenantField = fields[0];
            if (fields.length == NumberConstant.INT_1) {
                tenantLabelData = sourceData.filter(fields[0] + " = \'" + tenantId + "\'");
            } else if (fields.length == NumberConstant.INT_2) {
                tenantField = fields[1];
                tenantLabelData = sourceData.filter(fields[0] + " = \'" + dbRel.getAccountId() + "\'").filter(fields[1] + " = \'" + tenantId + "\'");
            } else {
                throw new JobInterruptedException("tenantRelFiled length greater than 2");
            }
            String groupKey = "filter_key";
            Dataset<Row> groupData = tenantLabelData.withColumn(groupKey, new org.apache.spark.sql.Column(tenantField));
            //进行二级拆分
            if (StringUtils.isNotBlank(bean.getSplitFieldEx())) {
                for (String filterField : bean.getSplitFieldEx().split(",")) {
                    LOGGER.info("filterField:" + filterField);
                    groupData = groupData.selectExpr("*", "concat(" + groupKey + ",'_', " + filterField + ") as filter_key_new").drop(groupKey).withColumnRenamed("filter_key_new", groupKey);
                    LOGGER.info("groupdata schema:");
                    groupData.printSchema();
                }
            }
            LOGGER.info("group data finally:" + sourceTable);
            groupData.show(BigInteger.TEN.intValue());
            List<Row> items = groupData.selectExpr(groupKey).distinct().collectAsList();
            LOGGER.info("items size:" + items.size() + items.toString());
            for (Row item : items) {
                String itemKey = item.get(BigInteger.ZERO.intValue()).toString();
                String childTable = bean.getDesTablePre() + "_" + itemKey;
                String childTableDb = dbRel.getDbName() + "." + childTable;
                LOGGER.info("ready to create table:" + childTableDb);
                boolean isAdd = false;
                //需要创建每个租户库中的标签表
                Dataset<Row> childData = groupData.filter(groupKey + "=\'" + itemKey + "\'").drop(groupKey);
                if (bean.getRebuilt() && sparkSession.catalog().tableExists(childTableDb)) {
                    String deleteTb = "DROP TABLE " + childTableDb;
                    LOGGER.info("child table is drop:" + deleteTb);
                    sparkSession.sql(deleteTb);
                }
                if (!sparkSession.catalog().tableExists(childTableDb)) {
                    //租户中的标签表不存在
                    LOGGER.info("table is new:" + childTableDb);
                    isAdd = true;
                    StringBuilder createSql = new StringBuilder("CREATE TABLE " + childTableDb + "(");
                    createSql.append(finalCreateSql);
                    LOGGER.info("create table sql :" + createSql.toString());
                    sparkSession.sql(createSql.toString());
                }
                LOGGER.info("处理分区字段信息" + childTable);
                //需要删除复合类型字段（array,map,struct）
                //保存数据
                String viewName = "tempViewTable_" + childTable;
                childData.createOrReplaceTempView(viewName);

                writeDataToHive(viewName, childTableDb, sparkSession, "dt", dataType);
                //保存元数据到mysql
                if (bean.isUpdate2Mysql()) {
                    writeTableToMysql(childTable, sparkSession, dbRel);
                }
                //保存处理日志
//                writeDealLogInfo(dbRel, sourceTable, childTable, childData.count(), isAdd, bean.getDataSource(), sparkSession);

            }

        }
        bean.setOutputMap(new HashMap<String, Object>());
        return new HashMap<String, Object>();
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
    private boolean writeDataToHive(String viewName, String tableName, SparkSession sparkSession, String dtField, SplitDataTypeEnum dataType) {
//        String viewName = "tempViewTable_" + tableName;
        LOGGER.info("tableName={}, dtField={},dataType={},viewName={}", tableName, dtField, dataType, viewName);

        if (!sparkSession.catalog().tableExists(tableName)) {
            LOGGER.error("hive table " + tableName + " is not exists");
            throw new JobInterruptedException(tableName + "不存在", "hive table " + tableName + " is not exists");
        }
        try {
            LOGGER.info("data type=" + dataType.name());
//            if (dataType.name().equals(SplitDataTypeEnum.PARTITIONED_INCRESCALE_ALLUPDATE.name()) ||
//                    dataType.equals(SplitDataTypeEnum.NONEPARTITIONED_FULLSCALE_ALLUPDATE_PARTITIONED.name())) {
//                //多分区覆盖需要清理历史数据
//                sparkSession.sql("TRUNCATE TABLE " + tableName);
//            }
            StringBuilder sql = new StringBuilder("INSERT OVERWRITE TABLE " + tableName);
            if (dataType.isDesPartitioned()) {
                sql.append(" PARTITION(" + dtField + ")");
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
     * @throws
     * @title getTenantRelInfo
     * @description 获取账号字段和库表之间关系（支持主子两种类型）
     * @author cdxiongmei
     * @param: dataSource
     * @updateTime 2021/7/19 上午11:25
     * @return: java.lang.String
     */
    private List<JnosRelInfo> getTenantRelInfo(SplitDataSourceEnum dataSource, Dataset<Row> sourceData, String tenantFields) throws SQLException, IOException {
        LOGGER.info("getTenantRelInfo dataSource=" + dataSource.getValue());
        List<JnosRelInfo> tenantRel = new ArrayList<JnosRelInfo>();
        if (dataSource.getValue().equalsIgnoreCase(SplitDataSourceEnum.CRM.getValue())) {
            String querySql = "select\n" +
                    "  account_id,\n" +
                    "  main_account_name,\n" +
                    "  crm_name,\n" +
                    "  db_id,\n" +
                    "  db_name\n" +
                    "from\n" +
                    "  (\n" +
                    "    select\n" +
                    "      account_id," +
                    "      id as db_id,\n" +
                    "      database_name as db_name,\n" +
                    "      main_account_name\n" +
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
            //TODO 测试代码
            while (retInfos.next()) {
                LOGGER.info("accountId={},mainAccountName={},crm_name={}, dbid={}, dbname={}", retInfos.getLong(1), retInfos.getString(2), retInfos.getString(3), retInfos.getLong(4), retInfos.getString(5));
                //CRM中tenantcode为门店账号id，tenantname为jnos主账号名称
                tenantRel.add(new JnosRelInfo(retInfos.getLong(1), retInfos.getString(2), retInfos.getString(3), retInfos.getString(2), retInfos.getLong(4), retInfos.getString(5)));
            }
//            tenantRel.add(new JnosRelInfo(10431L, "北京好邻居", "100007811", "北京好邻居", 1440951193437745153L, "ae_user1"));

            LOGGER.info("tenantRel:" + tenantRel.toString());
            DbManagerService.close(db2, stmt, null, retInfos);
        } else if (dataSource.getValue().equalsIgnoreCase(SplitDataSourceEnum.ACCOUNTID.getValue())) {
            String querySql =
                    "    select\n" +
                            "      account_id," +
                            "      main_account_name,\n" +
                            "      id as db_id,\n" +
                            "      database_name as db_name\n" +
                            "    from\n" +
                            "      ea_database\n" +
                            "    where\n" +
                            "      yn = 1\n";
            Connection db2 = DbManagerService.getConn(); //创建DBHelper对象
            Statement stmt = DbManagerService.stmt(db2);
            ResultSet retInfos = DbManagerService.executeQuery(stmt, querySql); //执行语句，得到结果集
            while (retInfos.next()) {
                LOGGER.info("accountId={},mainAccountName={}, dbid={}, dbname={}", retInfos.getLong(1), retInfos.getString(2), retInfos.getLong(3), retInfos.getString(4));
                //CRM中tenantcode为门店账号id，tenantname为jnos主账号名称
                tenantRel.add(new JnosRelInfo(retInfos.getLong(1), retInfos.getString(2), retInfos.getString(1), retInfos.getString(2), retInfos.getLong(3), retInfos.getString(4)));
            } //显示数据
//            tenantRel.add(new JnosRelInfo(10431L, "北京好邻居", "100007811", "北京好邻居", 1440951193437745153L, "ae_user1"));
        } else if (dataSource.getValue().equalsIgnoreCase(SplitDataSourceEnum.CA.getValue())) {
            List<Row> tenantRows = null;
            if (tenantFields.split(",").length == NumberConstant.INT_1) {
                tenantRows = sourceData.select(tenantFields).distinct().collectAsList();
            } else if (tenantFields.split(",").length == NumberConstant.INT_2) {
                tenantRows = sourceData.select(tenantFields.split(",")[0].trim(), tenantFields.split(",")[1].trim()).distinct().collectAsList();
            } else {
                throw new JobInterruptedException("tenantRelFiled length greater than 2");
            }
            List<JSON> tenantIds = new ArrayList<>();
            tenantRows.forEach(row -> {
                LOGGER.info("tenantFields=" + tenantFields + ",row=" + row.toString());
                JSONObject tenant = new JSONObject();
                if (tenantFields.split(",").length == NumberConstant.INT_2) {
                    tenant.put("accountId", row.get(NumberConstant.INT_0).toString());
                    tenant.put("uid", row.get(NumberConstant.INT_1).toString());
                } else if (tenantFields.split(",").length == NumberConstant.INT_1) {
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
                    tenantRel.add(new JnosRelInfo(eleJson.getLong("accountId"), eleJson.getString("mainAccountName"), eleJson.getString("uid"), eleJson.getString("name"), eleJson.getLong("dbId"), eleJson.getString("dbName")));
                });
            }
            LOGGER.info("tenantRel:" + tenantRel.toString());
        }
        return tenantRel;
    }

    /**
     * @throws
     * @title writeTableToMysql
     * @description 将拆分后的字表信息同步到mysql表中
     * @author cdxiongmei
     * @param: isRebuilt 是否表进行重建
     * @param: tableName
     * @param: sparkSession
     * @param: tenantInfo
     * @param: dataSource
     * @updateTime 2021/7/20 下午3:28
     */
    private void writeTableToMysql(String tableName, SparkSession sparkSession, JnosRelInfo tenantInfo) throws AnalysisException, IOException, SQLException {
        //查询表的最近更新时间和文件大小数据
        SplitDataBean bean = (SplitDataBean) getStepBean();
        //获取记录数
        SplitDataTypeEnum dataType = bean.getDataType();
        List<String> partitionCols = new ArrayList<String>();
        if (dataType.isDesPartitioned()) {
            //分区表
            partitionCols.add("dt");
        }
        //数据行数获取
        TableMetaInfo fileInfo = TableMetaUtil.getTargetTableMetaInfo(tenantInfo.getDbName(), tableName, sparkSession);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String now = simpleDateFormat.format(new Date());
        //先查询是否已经存在记录
        Long tableId = 0L;
        String tableExsit = "SELECT id FROM " + ConfigProperties.TABLE_INFO_TB() + " WHERE dataBase_name=\"" + tenantInfo.getDbName() + "\" and table_name=\"" + tableName + "\" and yn=1 and status=0";
        Connection conn = DbManagerService.getConn();
        Statement stmt = DbManagerService.stmt(conn);
        ResultSet ret = DbManagerService.executeQuery(stmt, tableExsit);
        while (ret.next()) {
            tableId = ret.getLong(1);
        }
        if (tableId > BigInteger.ZERO.intValue() && !bean.getRebuilt()) {
            //已经存在表，只需更新信息
            LOGGER.info("表信息已经存在，只需要更新tableId=" + tableId);
            String updateSql = "UPDATE " + ConfigProperties.TABLE_INFO_TB() + " SET table_storage_size=\"" + fileInfo.getfileSizeFormat() +
                    "\", table_data_update_time=\"" + fileInfo.getmodificationTimeFormat() +
                    "\",update_time=\"" + now +
                    "\",source=" + bean.getSourceTypeEx().getType() +
                    ",table_row_count=" + fileInfo.getrowNumber() +
                    " WHERE id=" + tableId;
            int updateRow = DbManagerService.executeUpdate(stmt, updateSql);
            if (updateRow > BigInteger.ZERO.intValue()) {
                LOGGER.info("数据更新成功");
            }
            DbManagerService.close(conn, stmt, null, null);
            return;

        } else if (tableId > BigInteger.ZERO.intValue() && bean.getRebuilt()) {
            //已经存在表，需要删除表信息和字段信息
            LOGGER.info("表信息已经存在，需要进行重建tableId=" + tableId);
            //删除表信息
            String deleteSql = "UPDATE " + ConfigProperties.TABLE_INFO_TB() + " SET yn=0,update_time=\"" + now + "\"  WHERE id=" + tableId;
            int deleteRow = DbManagerService.executeUpdate(stmt, deleteSql);
            if (deleteRow > BigInteger.ZERO.intValue()) {
                LOGGER.info("表数据删除成功");
            }
        }
        //新增表信息
        LOGGER.info("表信息不存在，需要更新表和字段信息");
        String inserttableSql = "INSERT INTO " + ConfigProperties.TABLE_INFO_TB() + "(account_id,main_account_name,sub_account_name, dataBase_name, dataBase_id,table_name,table_comment,table_description,table_storage_size, table_data_update_time,status,yn,create_time,update_time,source,table_row_count)\nVALUES (";
        inserttableSql += tenantInfo.getAccountId() + ",\"";
        inserttableSql += tenantInfo.getMainAccountName() + "\",\"";
        String subAccount = tenantInfo.getSubAccountName();
        if (bean.getDataSource().getValue().equalsIgnoreCase(SplitDataSourceEnum.CRM.getValue())) {
            subAccount = tenantInfo.getMainAccountName();
        }
        inserttableSql += subAccount + "\",\"";
        inserttableSql += tenantInfo.getDbName() + "\"," + tenantInfo.getDbId() + ",\"" + tableName + "\",\"";
        String tableComment = sparkSession.catalog().getTable(tenantInfo.getDbName(), tableName).description();
        if (StringUtils.isBlank(tableComment)) {
            tableComment = "";
        }
        inserttableSql += tableComment + "\",\"" + tableComment + "\",\"";
        LOGGER.info("fileInfo:" + fileInfo);
        inserttableSql += fileInfo.getfileSizeFormat() + "\",\"" + fileInfo.getmodificationTimeFormat() + "\",0,1,\"" + now + "\",\"" + now + "\"," + bean.getSourceTypeEx().getType() + "," + fileInfo.getrowNumber() + ");";
        LOGGER.info("ea_create_table_info sql:" + inserttableSql);
        int updateRow = DbManagerService.executeUpdate(stmt, inserttableSql);
        if (updateRow <= BigInteger.ZERO.intValue()) {
            LOGGER.error("ea_create_table_info数据新增失败");
        }
        //获取tableId
        ResultSet retId = DbManagerService.executeQuery(stmt, tableExsit);
        while (retId.next()) {
            tableId = retId.getLong(1);
        }
        System.out.println("tableId:" + tableId);
        DbManagerService.close(conn, stmt, null, retId);
        //新增字段信息
//        List<org.apache.spark.sql.catalog.Column> fieldInfos = sparkSession.catalog().listColumns(tenantInfo.getDbName() + "." + tableName).collectAsList();
//        PreparedStatement ps = conn.prepareStatement(
//                "INSERT INTO ea_create_table_field(table_id,main_account_name, sub_account_name, table_field_name, table_field_type,table_field_comment,table_dt_field, yn,create_time,update_time) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
//        for (org.apache.spark.sql.catalog.Column field : fieldInfos) {
//            ps.setLong(1, tableId);
//            ps.setString(2, tenantInfo.getMainAccount());
//            ps.setString(3, subAccount);
//            ps.setString(4, field.name());
//            ps.setString(5, field.dataType());
//            ps.setString(6, field.description() == null ? "" : field.description());
//            if (field.isPartition()) {
//                ps.setString(6, "分区字段日期格式yyyy-MM-dd");
//            }
//            ps.setInt(7, field.isPartition() ? BigInteger.ONE.intValue() : BigInteger.ZERO.intValue());
//            ps.setInt(8, 1);
//            ps.setTimestamp(9, new Timestamp(System.currentTimeMillis()));
//            ps.setTimestamp(10, new Timestamp(System.currentTimeMillis()));
//            ps.addBatch();
//        }
//        ps.executeBatch();
//        DbManagerService.close(conn, null, ps, null);
    }

    private String getDateStrBeforeNDays(Date date, int interval) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DATE, -interval);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormat.format(cal.getTime());
    }

    /**
     * @throws
     * @title writeDealLogInfo
     * @description 完成拆分后需要写入日志
     * @author cdxiongmei
     * @updateTime 2021/7/22 上午10:17
     */
    private boolean writeDealLogInfo(JnosRelInfo info, String sourceTable, String desTable, Long rowCnt, boolean isCreate, SplitDataSourceEnum sourceType, SparkSession session) {
        String cdpLogTable = ConfigProperties.CDPLOG_TABLE();
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println("max:" + sdf.format(date));

        String curDate = getDateStrBeforeNDays(date, 0);
        String insertSql = "INSERT INTO TABLE " + cdpLogTable + " PARTITION(dt=\'" + curDate + "\')\n";
        insertSql += "SELECT \"" + sourceTable + "\" source_tb,\n\"";
        insertSql += info.getDbName() + "\" des_db,\n\"";
        insertSql += desTable + "\" des_tb,\n";
        insertSql += rowCnt + " row_cnt,\n\"";
        insertSql += info.getMainAccountName() + "\" main_account,\n\"";
        insertSql += (sourceType.getValue().equals(SplitDataSourceEnum.CA.getValue()) ? info.getSubAccountName() : info.getMainAccountName()) + "\" sub_account,\n\"";
        insertSql += sourceType.getValue() + "\" source_type,\n";
        insertSql += (isCreate ? 1 : 2) + " event_type,\n\"";
        insertSql += sdf.format(date) + "\" event_time";
        LOGGER.info("writeDealLogInfo insertSql=" + insertSql);
        session.sql(insertSql);
        return true;
    }
}

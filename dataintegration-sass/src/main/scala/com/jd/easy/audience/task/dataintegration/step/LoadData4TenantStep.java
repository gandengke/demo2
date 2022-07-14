package com.jd.easy.audience.task.dataintegration.step;

import com.jd.easy.audience.common.constant.NumberConstant;
import com.jd.easy.audience.common.exception.JobInterruptedException;
import com.jd.easy.audience.common.util.JsonUtil;
import com.jd.easy.audience.task.commonbean.bean.LoadData4TenantBean;
import com.jd.easy.audience.task.commonbean.contant.SplitSourceTypeEnum;
import com.jd.easy.audience.task.dataintegration.property.ConfigProperties;
import com.jd.easy.audience.task.dataintegration.util.*;
import com.jd.easy.audience.task.driven.step.StepCommon;
import com.jd.easy.audience.task.driven.step.StepCommonBean;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Column;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

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
 * @description 根据指定的库进行待拆分数据的落库
 * @updateTime 2021/7/14 下午3:05
 * @throws
 */
@Deprecated
public class LoadData4TenantStep extends StepCommon<Map<String, Object>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoadData4TenantStep.class);
    private static final List<String> IGNOR_FIELDS = Arrays.asList("sub_table_name,account_info,source_type_ex,table_comment".split(","));

    @Override
    public void validate() {
        super.validate();
        LoadData4TenantBean bean = (LoadData4TenantBean) getStepBean();
        LOGGER.info("bean info:" + JsonUtil.serialize(bean));

        if (StringUtils.isBlank(bean.getSourceTableName())) {
            LOGGER.error("拆分数据必须要指定源表和目标表前缀");
            throw new JobInterruptedException("拆分数据必须要指定源表和目标表前缀", "split data must have source table and target table");
        }
    }

    @Override
    public Map<String, Object> run(Map<String, StepCommonBean> map) throws Exception {
        //1.取出租户和hive数据库的关联关系
        LoadData4TenantBean bean = (LoadData4TenantBean) getStepBean();
        SparkSession sparkSession = SparkUtil.buildMergeFileSession("LoadData4TenantStep_" + bean.getAccountId() + "_" + bean.getSourceTableName());
        String sourceTable = ConfigProperties.PUBLIC_DB_NAME() + "." + bean.getSourceTableName();
        String buffaloNtime = System.getenv(ConfigProperties.BUFFALO_ENV_NTIME());
        LOGGER.info("buffaloNtime:" + buffaloNtime);
        DateTime jobNTime = DateTime.parse(buffaloNtime, DateTimeFormat.forPattern("yyyyMMddHHmmss"));
        String proAccountName = System.getenv(ConfigProperties.HADOOP_USER()); //生产账号

        LOGGER.info("buffaloNtime:{}, cycleType= {}, circleStep={}, sourceTable={}", buffaloNtime, bean.getCircleType(), bean.getCircleStep(), bean.getSourceTableName());
        //过滤字段加工
        String fieldSelectedTemp = bean.getFieldFilter();
        Tuple2<String[], String[]> partitionInfos = DataFrameOps.getColumns(sourceTable, sparkSession);
        boolean isDtPartitioned = Arrays.asList(partitionInfos._2).contains("dt");
        if (StringUtils.isBlank(fieldSelectedTemp)) {
            fieldSelectedTemp = "";
            for (int i = 0; i < partitionInfos._1.length; i++) {
                if (!IGNOR_FIELDS.contains(partitionInfos._1[i])) {
                    fieldSelectedTemp += "," + partitionInfos._1[i];
                }
            }
            if (null != partitionInfos._2 && partitionInfos._2.length > NumberConstant.INT_0 && isDtPartitioned) {
                fieldSelectedTemp += ",dt";
            }
        }
        String fieldSelected = fieldSelectedTemp.substring(1);
        LOGGER.info("FieldFilter:" + fieldSelected + ", FilterExpress" + bean.getFilterExpress());
        Dataset<Row> dataSource = SparkUtil.printAndExecuteSql("SELECT * FROM " + ConfigProperties.PUBLIC_DB_NAME() + "." + bean.getSourceTableName() + " WHERE " + bean.getFilterExpress(), sparkSession);
        dataSource.cache();
        dataSource.show(10);
        //hdfs路径，解析分区数据
//        Dataset<Row> subDataSource = dataSource.filter(bean.getFilterExpress());
//        subDataSource.cache();
        Row firstRow = dataSource.first();
        int sourceTypeEx = Integer.valueOf(firstRow.getAs(IGNOR_FIELDS.get(2))).intValue();
        String tableComment = firstRow.getAs(IGNOR_FIELDS.get(3));
        String tableNameDb = firstRow.getAs(IGNOR_FIELDS.get(0));
        String accountNameInfo = firstRow.getAs(IGNOR_FIELDS.get(1));
        if (sparkSession.catalog().tableExists(tableNameDb) && bean.getIsRebuilt()) {
            String deleteTb = "DROP TABLE " + tableNameDb;
            SparkUtil.printAndExecuteSql(deleteTb, sparkSession);
        }
        if (!sparkSession.catalog().tableExists(tableNameDb)) {
            //创建表
            createTableSql(Arrays.asList(fieldSelected.split(",")), tableNameDb, sparkSession, sourceTable);
        }
        //处理数据
        dataSource.createOrReplaceTempView("tmp_view_his");
        SparkUtil.printAndExecuteSql("SELECT " + fieldSelected + " FROM tmp_view_his", sparkSession).toDF().createOrReplaceTempView("sub_data_source");
        writeDataToHive("sub_data_source", tableNameDb, sparkSession, isDtPartitioned);
        if (bean.isUpdate2Mysql()) {
            writeTableToMysql(tableNameDb, sparkSession, isDtPartitioned, bean.getIsRebuilt(), accountNameInfo, tableComment, sourceTypeEx);
        }
        sparkSession.catalog().dropTempView("tmp_view_his");
        sparkSession.catalog().dropTempView("sub_data_source");

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
    private boolean writeDataToHive(String viewName, String tableName, SparkSession sparkSession, boolean isDtPartitioned) {
//        String viewName = "tempViewTable_" + tableName;
        LOGGER.info("tableName={},isDtPartitioned={},viewName={}", tableName, isDtPartitioned, viewName);
        if (!sparkSession.catalog().tableExists(tableName)) {
            LOGGER.error("hive table " + tableName + " is not exists");
            throw new JobInterruptedException(tableName + "不存在", "hive table " + tableName + " is not exists");
        }
        try {
            StringBuilder sql = new StringBuilder("INSERT OVERWRITE TABLE " + tableName);
            if (isDtPartitioned) {
                sql.append(" PARTITION(dt)");
            }
            sql.append("\nSELECT * \nFROM\n" + viewName);
            SparkUtil.printAndExecuteSql(sql.toString(), sparkSession);

        } catch (Exception e) {
            LOGGER.error("writeDataToHive exception!", e);
            return false;
        }
        return true;
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
    private void writeTableToMysql(String subTableName, SparkSession sparkSession, boolean isDtPartitioned, boolean isRebuild, String accountInfo, String tableComment, int sourceTypeEx) throws IOException, SQLException, AnalysisException {
        LOGGER.info("subTableName={},isDtPartitioned={},isRebuild={},accountInfo={}, sourceTypeEx={}", subTableName, isDtPartitioned, isRebuild, accountInfo, sourceTypeEx);
        String[] accountArr = accountInfo.split("_");
        if (accountArr.length < 2) {
            throw new JobInterruptedException("账号信息异常");
        }
        String accountId = accountArr[0];
        String accountName = accountArr[1];
        List<String> partitionCols = new ArrayList<String>();
        if (isDtPartitioned) {
            //分区表
            partitionCols.add("dt");
        }
        //数据行数获取
        String dbName = subTableName.split("\\.")[0];
        String targetTb = subTableName.split("\\.")[1];
        TableMetaInfo fileInfo = TableMetaUtil.getTargetTableMetaInfo(dbName, targetTb, sparkSession);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String now = simpleDateFormat.format(new Date());
        String modifiedTime = now;
        //先查询是否已经存在记录
        Long tableId = 0L;
        String tableExsit = "SELECT id FROM " + ConfigProperties.TABLE_INFO_TB() + " WHERE dataBase_name=\"" + dbName + "\" and table_name=\"" + targetTb + "\" and yn=1 and status=0";
        Connection conn = DbManagerService.getConn();
        Statement stmt = DbManagerService.stmt(conn);
        ResultSet ret = DbManagerService.executeQuery(stmt, tableExsit);
        while (ret.next()) {
            tableId = ret.getLong(1);
        }
        if (tableId > BigInteger.ZERO.intValue() && !isRebuild) {
            //已经存在表，只需更新信息
            LOGGER.info("表信息已经存在，只需要更新tableId=" + tableId);
            if (StringUtils.isNotBlank(fileInfo.getmodificationTimeFormat())) {
                modifiedTime = fileInfo.getmodificationTimeFormat();
            }
            String updateSql = "UPDATE " + ConfigProperties.TABLE_INFO_TB() + " SET table_storage_size=\"" + fileInfo.getfileSizeFormat() +
                    "\", table_data_update_time=\"" + modifiedTime +
                    "\",update_time=\"" + now +
                    "\",source=" + sourceTypeEx +
                    ",table_row_count=" + fileInfo.getrowNumber() +
                    " WHERE id=" + tableId;
            int updateRow = DbManagerService.executeUpdate(stmt, updateSql);
            if (updateRow > BigInteger.ZERO.intValue()) {
                LOGGER.info("数据更新成功");
            }
            DbManagerService.close(conn, stmt, null, null);
            return;

        } else if (tableId > BigInteger.ZERO.intValue() && isRebuild) {
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
        String inserttableSql = "INSERT INTO " + ConfigProperties.TABLE_INFO_TB() + "(account_id,main_account_name,sub_account_name, dataBase_name,table_name,table_comment,table_description,table_storage_size, table_data_update_time,status,yn,create_time,update_time,source,table_row_count)\nVALUES (";
        inserttableSql += accountId + ",\"";
        inserttableSql += accountName + "\",\"";
        String subAccount = accountArr.length == 3 ? accountArr[2] : accountName;
        inserttableSql += subAccount + "\",\"";
        inserttableSql += dbName + "\",\"" + targetTb + "\",\"";
        if (StringUtils.isBlank(tableComment)) {
            tableComment = "";
        }
        inserttableSql += tableComment + "\",\"" + tableComment + "\",\"";
        LOGGER.info("fileInfo:" + fileInfo);
        inserttableSql += fileInfo.getfileSizeFormat() + "\",\"" + modifiedTime + "\",0,1,\"" + now + "\",\"" + now + "\"," + sourceTypeEx + "," + fileInfo.getrowNumber() + ");";
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
    private boolean writeDealLogInfo(String accountInfo, String sourceTable, String desTable, Long rowCnt, boolean isCreate, SplitSourceTypeEnum sourceType, SparkSession session) {
        String[] accountArr = accountInfo.split("_");
        if (accountArr.length < 2) {
            throw new JobInterruptedException("账号信息异常");
        }
        String accountId = accountArr[0];
        String accountName = accountArr[1];

        String cdpLogTable = ConfigProperties.CDPLOG_TABLE();
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println("max:" + sdf.format(date));

        String curDate = getDateStrBeforeNDays(date, 0);
        String insertSql = "INSERT INTO TABLE " + cdpLogTable + " PARTITION(dt=\'" + curDate + "\')\n";
        insertSql += "SELECT \"" + sourceTable + "\" source_tb,\n\"";
        insertSql += desTable.split("\\.")[0] + "\" des_db,\n\"";
        insertSql += desTable.split("\\.")[1] + "\" des_tb,\n";
        insertSql += rowCnt + " row_cnt,\n\"";
        insertSql += accountName + "\" main_account,\n\"";
        insertSql += accountArr[2] + "\" AS sub_account,\n\"";
        insertSql += sourceType.getDesc() + "\" source_type,\n";
        insertSql += (isCreate ? 1 : 2) + " event_type,\n\"";
        insertSql += sdf.format(date) + "\" event_time";
        SparkUtil.printAndExecuteSql(insertSql, session);
        return true;
    }

    /**
     * 创建字表
     *
     * @param selectedFieldsList
     * @param desTable
     * @param sparkSession
     * @param sourceTable
     * @throws AnalysisException
     */
    private void createTableSql(List<String> selectedFieldsList, String desTable, SparkSession sparkSession, String sourceTable) throws AnalysisException {
        LOGGER.info("createTableSql READY TO START!selectedFieldsList=" + selectedFieldsList + ",sourceTable=" + sourceTable);
        StringBuilder finalCreateSql = new StringBuilder("CREATE TABLE " + desTable + "(");
        List<Column> sourceCols = sparkSession.catalog().listColumns(sourceTable).collectAsList();
        for (Column field : sourceCols) {
            LOGGER.info("fieldname=" + field.name());
            if (selectedFieldsList.contains(field.name()) && !"dt".equalsIgnoreCase(field.name())) {
                //过滤配置中的字段且非分区字
                finalCreateSql.append(field.name() + " " + field.dataType() + (StringUtils.isBlank(field.description()) ? "," : (" COMMENT \'" + field.description() + "\',")));
            }
        }
        finalCreateSql = new StringBuilder(finalCreateSql.substring(0, finalCreateSql.length() - 1));
        //新增标示字段
        if (selectedFieldsList.contains("dt")) {
            //有时间分区字段
            finalCreateSql.append(") PARTITIONED BY(dt STRING");
        }
        finalCreateSql.append(") STORED AS ORC tblproperties('orc.compress'='SNAPPY')");
        SparkUtil.printAndExecuteSql(finalCreateSql.toString(), sparkSession);
    }
}

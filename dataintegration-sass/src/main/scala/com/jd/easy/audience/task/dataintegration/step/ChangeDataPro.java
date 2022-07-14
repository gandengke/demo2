package com.jd.easy.audience.task.dataintegration.step;

import com.jd.easy.audience.common.constant.NumberConstant;
import com.jd.easy.audience.task.dataintegration.property.ConfigProperties;
import com.jd.easy.audience.task.dataintegration.util.DataFrameOps;
import com.jd.easy.audience.task.dataintegration.util.SparkUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;

/**
 * @Author: cdxiongmei
 * @Date: 2021/7/9 上午10:51
 * @Description: 修改对应库表权限
 */
@Deprecated
public class ChangeDataPro {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeDataPro.class);

    public static void main(String[] args) throws AnalysisException, IOException {
        LOGGER.info("参数个数" + args.length);
        String dbName = args[0];
        String proAccountName = System.getenv(ConfigProperties.HADOOP_USER()); //生产账号
        LOGGER.info("dbName={}", dbName);
        SparkSession sparkSession = SparkUtil.buildMergeFileSession("ChangeDataPro");
        FileSystem fileSystem = FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());
        LOGGER.info("applicationId:" + sparkSession.sparkContext().applicationId());
        List<Table> tables = sparkSession.catalog().listTables(dbName).collectAsList();
        LOGGER.info("表的数量：" + tables.size());
        for (Table table : tables) {
            String tableName = table.name();

            Dataset<Row> data = SparkUtil.printAndExecuteSql("desc formatted " + dbName + "." + tableName, sparkSession);
            String owner = data.filter("col_name='Owner'").first().getAs("data_type").toString();
            String location = data.filter("col_name='Location'").first().getAs("data_type").toString();
            LOGGER.info("表的名称：" + table.name() + ", OWNER=" + owner + ", location=" + location);
            if (!owner.equalsIgnoreCase(proAccountName)) {
                LOGGER.info("表的名称：" + table.name() + " change owner");
                SparkUtil.printAndExecuteSql("create table " + dbName + "." + tableName + "_newnamename like " + dbName + "." + tableName, sparkSession);
                Tuple2<String[], String[]> fields = DataFrameOps.getColumns(dbName + "." + tableName, sparkSession);
                String partitionInfo = "";
                if (null != fields._2 && fields._2.length > NumberConstant.INT_0) {
                    partitionInfo = "PARTITION(" + StringUtils.join(fields._2, ",") + ")";
                }
                SparkUtil.printAndExecuteSql("INSERT OVERWRITE TABLE " + dbName + "." + tableName + "_newnamename " + partitionInfo + " \nSELECT * FROM " + dbName + "." + tableName, sparkSession);

                SparkUtil.printAndExecuteSql(" \nDROP TABLE " + dbName + "." + tableName, sparkSession);
                fileSystem.delete(new Path(location), true);
                SparkUtil.printAndExecuteSql(" \nALTER TABLE " + dbName + "." + tableName + "_newnamename RENAME TO " + dbName + "." + tableName, sparkSession);
            }
            data.show();
        }
    }
}

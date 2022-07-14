package com.jd.easy.audience.task.plugin.run.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * test
 *
 * @author cdxiongmei
 * @version V1.0
 */
public class ReadOssDemo {
    public static void main(String[] args) {
        String endpoint = args[0];
        String mountPath = args[1];
        String outputPath = args[2];
        System.out.println("endPoint:" + endpoint + ", mountPath:" + mountPath + ", outputpath:" + outputPath);
        SparkSession sparkSession = SparkLauncherUtil.buildSparkSession("TestReadStep");
        Dataset<Row> dataRead = sparkSession.read().csv(mountPath);
        dataRead.show();
        dataRead.schema();
        Dataset<Row> data = sparkSession.createDataFrame(initData2(), initSchema()).repartition(1);
        data.cache();
        data.show();
        data.write().option("header", "true").csv("hdfs://ns1/user/cdp_biz-org.bdp.cs/cfs/shared" + outputPath);
    }
    public static java.util.List<Row> initData2() {
        java.util.List<Row> dataa = new java.util.ArrayList<Row>();
        dataa.add(RowFactory.create("account_id1", "uid_1", "111"));
        dataa.add(RowFactory.create("account_id1", "uid_1", "222"));
        dataa.add(RowFactory.create("account_id2", "uid_1", "333"));
        return dataa;
    }

    public static StructType initSchema() {
        List<StructField> fieldList = new ArrayList<>();
        fieldList.add(DataTypes.createStructField("labelName", DataTypes.StringType, false));
        fieldList.add(DataTypes.createStructField("labelValue", DataTypes.StringType, false));
        fieldList.add(DataTypes.createStructField("account_id", DataTypes.StringType, false));
        StructType struct = DataTypes.createStructType(fieldList);
        return struct;
    }
}

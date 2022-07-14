package com.jd.easy.audience.task.plugin.run.spark;

import com.jd.easy.audience.common.constant.StringConstant;
import com.jd.easy.audience.common.exception.JobInterruptedException;
import com.jd.easy.audience.common.oss.OssClientTypeEnum;
import com.jd.easy.audience.common.oss.OssConfig;
import com.jd.easy.audience.common.util.JdCloudOssUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * SparkLauncherUtiln 每个模块可能有自己的配置
 *
 * @author guoliming
 * @date -
 */
public class SparkLauncherUtil {
    /**
     * http链接的协议头
     */
    private static final String HTTP_HEAD = "http://";

    /**
     * 创建spark session带有ES相关配置，但不需要有es用户名密码
     */
    @Deprecated
    public static SparkSession buildSparkSessionWithESSettings(String esNodes, String esPort, String appName) {
        return buildSparkSessionWithESSettings(esNodes, esPort, null, null, appName);
    }

    /**
     * 创建spark session带有ES相关配置
     */
    @Deprecated
    public static SparkSession buildSparkSessionWithESSettings(String esNodes, String esPort, String esUser, String esPass, String appName) {
        if (esNodes.startsWith(HTTP_HEAD)) {
            esNodes = esNodes.replaceAll(HTTP_HEAD, "");
        }
        //
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("es.port", esPort);
        sparkConf.set("es.nodes", esNodes);
        sparkConf.set("es.index.auto.create", "true");
        if (!isNullOrEmpty(esUser)) {
            sparkConf.set("es.net.http.auth.user", esUser);
        }
        if (!isNullOrEmpty(esPass)) {
            sparkConf.set("es.net.http.auth.pass", esPass);
        }

        return new SparkSession.Builder()
                .master("yarn")
                .appName(appName)
                .config(sparkConf)
                .config("hive.exec.orc.default.stripe.size", 268435456L)
                .config("hive.exec.orc.split.strategy", "BI")
                .config("spark.sql.broadcastTimeout", "36000")
                .config("mapred.compress.map.output", "true")
                .config("mapred.output.compression.type", "BLOCK")
                .config("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
                .enableHiveSupport()
                .getOrCreate();
    }

    public static SparkSession buildSparkSessionWithESSettingsMultiNodes(String esNodes, String esPort, String esUser, String esPass, String appName) {
        if (esNodes.startsWith(HTTP_HEAD)) {
            esNodes = esNodes.replaceAll(HTTP_HEAD, "");
        }
        SparkConf sparkConf = new SparkConf();
        String esNodesConfig = "";
        if (esNodes.contains(StringConstant.COLON)) {
            esNodesConfig = esNodes;
        } else if (StringUtils.isNotBlank(esPort)) {
            esNodesConfig = esNodes + StringConstant.COLON + esPort;
        } else {
            throw new JobInterruptedException("ES config port config is empty", "ES config port config is empty");
        }
        sparkConf.set("es.index.auto.create", "true");
        sparkConf.set("es.nodes", esNodesConfig);
        if (!isNullOrEmpty(esUser)) {
            sparkConf.set("es.net.http.auth.user", esUser);
        }
        if (!isNullOrEmpty(esPass)) {
            sparkConf.set("es.net.http.auth.pass", esPass);
        }
//        sparkConf.set("es.nodes.discovery", "true");
        sparkConf.set("es.nodes.wan.only", "true");
//        System.out.println("sparkconfig:" + sparkConf.toDebugString());
        return new SparkSession.Builder()
                .master("yarn")
                .appName(appName)
                .config(sparkConf)
                .config("hive.exec.orc.default.stripe.size", 268435456L)
                .config("hive.exec.orc.split.strategy", "BI")
                .config("spark.sql.broadcastTimeout", "36000")
                .config("mapred.compress.map.output", "true")
                .config("mapred.output.compression.type", "BLOCK")
                .config("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
                .enableHiveSupport()
                .getOrCreate();
    }
    /**
     * 创建spark session带有oss相关配置
     */
    public static SparkSession buildSparkSessionWithOss(String appName, String endpoint, String accessKey, String secretKey, OssClientTypeEnum ossType) {
        SparkSession sparkSession = buildSparkSession(appName);
        Configuration configuration = sparkSession.sparkContext().hadoopConfiguration();
        OssConfig config = new OssConfig(endpoint, accessKey, secretKey);
        JdCloudOssUtil.initOssConf(configuration, config, ossType);
        return sparkSession;
    }
    public static SparkSession buildSparkSessionWithOssTest(String appName, String endpoint, String accessKey, String secretKey, OssClientTypeEnum ossType) {
        SparkSession sparkSession = new SparkSession.Builder()
                .master("yarn")
                .appName(appName)
                .config("spark.sql.parquet.writeLegacyFormat", true)
                .config("spark.hadoop.hive.exec.dynamic.partition", "true")
                .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
                .config("spark.sql.hive.mergeFiles", "true")
                .config("spark.hadoop.hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
                .config("spark.hadoop.hive.hadoop.supports.splittable.combineinputformat", "true")
                .config("spark.hadoop.mapreduce.input.fileinputformat.split.maxsize", "256000000")
                .config("spark.hadoop.mapreduce.input.fileinputformat.split.minsize.per.node", "256000000")
                .config("spark.hadoop.hive.exec.max.dynamic.partitions", "2000")
                .config("spark.hadoop.mapreduce.input.fileinputformat.split.minsize.per.rack", "256000000")
                .config("spark.hadoop.hive.merge.mapfiles", "true")
                .config("spark.hadoop.hive.merge.mapredfiles", "true")
                .config("spark.hadoop.hive.merge.size.per.task", "256000000")
                .config("spark.hadoop.hive.merge.smallfiles.avgsize", "256000000")
                .enableHiveSupport()
                .getOrCreate();
        Configuration configuration = sparkSession.sparkContext().hadoopConfiguration();
        configuration.set("fs.jfs.impl", "com.jd.easy.audience.task.plugin.util.jfs.JFSFileSystem");
        configuration.set("fs.jfs.endPoint", endpoint);
        configuration.set("fs.jfs.accessKey", accessKey);
        configuration.set("fs.jfs.secretKey", secretKey);
        configuration.set("fs.jfs.block.size", "67108864");
        configuration.set("fs.jfs.skip.size", "1048576");
        return sparkSession;
    }

    /**
     * 创建spark session带有oss相关配置
     */
    public static SparkSession buildSparkSession(String appName) {
        return new SparkSession.Builder()
                .master("yarn")
                .appName(appName)
                .config("spark.sql.parquet.writeLegacyFormat", true)
                .config("spark.hadoop.hive.exec.dynamic.partition", "true")
                .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
                .config("spark.sql.hive.mergeFiles", "true")
                .config("spark.hadoop.hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
                .config("spark.hadoop.hive.hadoop.supports.splittable.combineinputformat", "true")
                .config("spark.hadoop.mapreduce.input.fileinputformat.split.maxsize", "256000000")
                .config("spark.hadoop.mapreduce.input.fileinputformat.split.minsize.per.node", "256000000")
                .config("spark.hadoop.hive.exec.max.dynamic.partitions", "2000")
                .config("spark.hadoop.mapreduce.input.fileinputformat.split.minsize.per.rack", "256000000")
                .config("spark.hadoop.hive.merge.mapfiles", "true")
                .config("spark.hadoop.hive.merge.mapredfiles", "true")
                .config("spark.hadoop.hive.merge.size.per.task", "256000000")
                .config("spark.hadoop.hive.merge.smallfiles.avgsize", "256000000")
                .enableHiveSupport()
                .getOrCreate();
    }
    /**
     * 合并df
     *
     * @param spark    SparkSession
     * @param parentDf Dataset<Row>
     * @param df       Dataset<Row>
     * @return
     */
    public static Dataset<Row> dfUnion(SparkSession spark, Dataset<Row> parentDf, Dataset<Row> df) {
        if (parentDf.equals(spark.emptyDataFrame())) {
            return df;
        } else {
            return parentDf.unionAll(df);
        }
    }

}

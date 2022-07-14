package test;

import com.google.common.collect.Lists;
import com.jd.easy.audience.common.error.SparkApplicationErrorResult;
import com.jd.easy.audience.common.exception.JobInterruptedException;
import com.jd.easy.audience.common.exception.JobNeedReTryException;
import com.jd.easy.audience.common.util.JsonUtil;
import com.jd.easy.audience.task.commonbean.segment.SparkStepSegment;
import com.jd.easy.audience.task.driven.segment.Segment;
import com.jd.easy.audience.task.driven.segment.SegmentFactory;
import com.jd.easy.audience.task.plugin.step.exception.StepExceptionEnum;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author cdxiongmei
 * @version 1.0
 * @description oneid关联创建标签数据集验证
 * @date 2022/5/31 1:52 PM
 */
public class OneIdMockData {
    private static final Logger LOGGER = LoggerFactory.getLogger(OneIdMockData.class);
    private static final Pattern DB_TABLE_SPLIT = Pattern.compile("^[a-zA-Z0-9|_]{1,10}\\.([_|\\w]*){1}(\\.[_|\\w]*)");

    public static void main(String[] args) {
        SparkSession sparkSession = new SparkSession.Builder()
                .master("local")
                .appName("OneIdUddTest")
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
                .getOrCreate();
        SparkContext sc = sparkSession.sparkContext();
        sc.setLogLevel("ERROR");
//        Dataset<Row> data = sparkSession.read().format("orc").load("file:///Users/cdxiongmei/Downloads/1656573392903119_1656574657387925.orc");
//        Dataset<Row> data = sparkSession.read().format("orc").load("file:///Users/cdxiongmei/Downloads/1655102919519170_1655103814193394.orc");
//        Dataset<Row> data = sparkSession.read().format("csv").load("file:///Users/cdxiongmei/Downloads/audiencexm.csv");
        Dataset<Row> data = sparkSession.read().format("orc").load("file:///Users/cdxiongmei/jd_import/qingteng2dataDir/1656573392903119_1656578634634988.orc");
        System.out.println(data.count());
//        mockData(sparkSession);
    }

    private static void dealError(Exception e) {
        if (e.getCause() == null) {
            //5.1 系统错误
            buildAppError(new JobNeedReTryException("系统错误", "Error: System Error!"));
            throw new RuntimeException("Wrong pipeline running", e);
        }
        String className = e.getCause().getClass().getSimpleName();
        if (e.getCause() instanceof JobInterruptedException) {
            //5.2 已经处理的异常-无需重试
            buildAppError((JobInterruptedException) e.getCause());
        } else if (e.getCause() instanceof JobNeedReTryException) {
            //5.3 已经处理的异常-需重试
            buildAppError((JobNeedReTryException) e.getCause());
        } else {
            //5.4 未处理异常，需要输出具体的异常内容
            StackTraceElement stackTraceElement = e.getCause().getStackTrace()[BigInteger.ZERO.intValue()];
            LOGGER.error("xmcauseby: ", e.getCause());
            LOGGER.error("异常名：" + stackTraceElement.toString());
            LOGGER.error("异常类名：" + stackTraceElement.getFileName());
            LOGGER.error("异常方法名：" + stackTraceElement.getMethodName());
            String stepName = stackTraceElement.getFileName().split("\\.")[BigInteger.ZERO.intValue()];
            String methodName = stackTraceElement.getMethodName();
            StepExceptionEnum exceptionEnum = StepExceptionEnum.valueOf(stepName, methodName);
            Exception exception = null;
            if (null != exceptionEnum && exceptionEnum.isNeedRetry()) {
                exception = new JobNeedReTryException(exceptionEnum.getExceptionDesc(), exceptionEnum.getExceptionEnDesc(), e);
            } else if (null != exceptionEnum && !exceptionEnum.isNeedRetry()) {
                exception = new JobInterruptedException(exceptionEnum.getExceptionDesc(), exceptionEnum.getExceptionEnDesc(), e);
            } else {
                exception = new JobNeedReTryException("系统错误", "Error: System Error!", e.getCause());
            }
            buildAppError(exception);
        }
        if (!(e.getCause() instanceof JobInterruptedException)) {
            //5.3 已经处理的异常-需重试
            throw new RuntimeException("Wrong pipeline running", e);
        }
    }

    private static SparkStepSegment getsparkstepInfo(String argPath) {
        SparkStepSegment sparkSegment = null;
        String segment = "";
        try {
            //2.1
            System.out.println(OneIdMockData.class.getResource("").getPath());
            File file = new File(OneIdMockData.class.getResource("").getPath() + argPath);
            if (file.exists()) {
                try {
                    // 将文件转换成String
                    segment = FileUtils.readFileToString(file, "UTF-8");

                } catch (Exception e) {
                    LOGGER.warn("readFileToString fail", e);
                }
            }
            Segment seg = SegmentFactory.create(segment);
            if (seg instanceof SparkStepSegment) {
                sparkSegment = (SparkStepSegment) seg;
            } else {
                LOGGER.warn("segment is wrong segment={}", seg.toString());
                throw new RuntimeException("segment is wrong");
            }
        } catch (Exception e) {
            LOGGER.error("DatamillSparkPipelineDriven deserialize error: ", e);
            throw new RuntimeException("Wrong pipeline segment define", e);
        }
        return sparkSegment;
    }

    /**
     * @throws
     * @title buildAppError
     * @description 构建作业的错误信息并上传至jfs
     * @author cdxiongmei
     * @param: jfsClient
     * @param: e
     * @param: applicationArgs
     * @updateTime 2021/8/10 上午10:42
     */
    private static void buildAppError(Exception e) {
        SparkApplicationErrorResult errorResult = new SparkApplicationErrorResult();
        errorResult.setErrorClass(e.getClass().toString());
        errorResult.setErrorMsg(e.getMessage());
        errorResult.setStackTraceJson(JsonUtil.serialize(e));
        if (e instanceof JobNeedReTryException) {
            JobNeedReTryException exception = (JobNeedReTryException) e;
            errorResult.setErrorEnMsg(exception.getMessageEn());
        } else if (e instanceof JobInterruptedException) {
            JobInterruptedException exception = (JobInterruptedException) e;
            errorResult.setErrorEnMsg(exception.getMessageEn());
        } else {
            errorResult.setErrorEnMsg("Error: System Error!");
        }
        LOGGER.error("errorResult string={}", JsonUtil.serialize(errorResult));
    }

    private static void mockData(SparkSession sparkSession) {
        sparkSession.createDataFrame(initDatauser8(), initSchemauser8())
                .repartition(1).write().option("header", "false").format("csv").save("file:///Users/cdxiongmei/jd_import/qingteng2dataDir/oneid_mockdata/user_8_9");
        sparkSession.createDataFrame(initDatauser9(), initSchemauser9())
                .repartition(1).write().option("header", "false").format("csv").save("file:///Users/cdxiongmei/jd_import/qingteng2dataDir/oneid_mockdata/user_9_10");
        sparkSession.createDataFrame(initDatauseroinid(), initSchemauseroneid())
                .repartition(1).write().option("header", "false").format("csv").save("file:///Users/cdxiongmei/jd_import/qingteng2dataDir/oneid_mockdata/oneid");
    }

    private static StructType initSchemauser8() { //        StructField id = DataTypes.createStructField("id", DataTypes.IntegerType, true);
        StructField venderId = DataTypes.createStructField("user_8", DataTypes.StringType, true);
        StructField venderId2 = DataTypes.createStructField("user_9", DataTypes.StringType, true);

        StructField latestTradeDate = DataTypes.createStructField("latest_trade_date", DataTypes.StringType, true);
        StructField orderQtty = DataTypes.createStructField("order_qtty", DataTypes.StringType, true);
        StructField tradeMoneySummary = DataTypes.createStructField("trade_money_summary", DataTypes.StringType, true);
        StructField dt = DataTypes.createStructField("dt", DataTypes.StringType, true);
        return DataTypes.createStructType(Lists.newArrayList(venderId, venderId2, latestTradeDate, orderQtty, tradeMoneySummary, dt));
    }

    private static List<Row> initDatauser8() {
        List<Row> data = new ArrayList<Row>();
        for (int i = 0; i < 2000; i++) {
            data.add(RowFactory.create("user_8_" + i, i % 300 == 0 ? "" : "user_9_" + i, "2022-02-17", "168", "28521.68768", "2022-06-29"));
        }
        return data;
    }

    private static StructType initSchemauser9() { //        StructField id = DataTypes.createStructField("id", DataTypes.IntegerType, true);
        StructField venderId = DataTypes.createStructField("user_10", DataTypes.StringType, true);
        StructField venderId2 = DataTypes.createStructField("user_9", DataTypes.StringType, true);

        StructField latestTradeDate = DataTypes.createStructField("latest_trade_date", DataTypes.StringType, true);
        StructField orderQtty = DataTypes.createStructField("order_qtty", DataTypes.StringType, true);
        StructField tradeMoneySummary = DataTypes.createStructField("trade_money_summary", DataTypes.StringType, true);
        StructField dt = DataTypes.createStructField("dt", DataTypes.StringType, true);
        return DataTypes.createStructType(Lists.newArrayList(venderId, venderId2, latestTradeDate, orderQtty, tradeMoneySummary, dt));
    }

    private static List<Row> initDatauser9() {
        List<Row> data = new ArrayList<Row>();
        for (int i = 0; i < 5000; i++) {
            data.add(RowFactory.create("user_10_" + i, i % 200 == 0 ? "" : "user_9_" + i, "2022-02-17", "168", "28521.68768", "2022-06-29"));
        }
        return data;
    }

    private static StructType initSchemauseroneid() { //        StructField id = DataTypes.createStructField("id", DataTypes.IntegerType, true);
        StructField venderId = DataTypes.createStructField("id", DataTypes.StringType, true);
        StructField venderId2 = DataTypes.createStructField("id_type", DataTypes.StringType, true);

        StructField userId = DataTypes.createStructField("one_id", DataTypes.StringType, true);
        return DataTypes.createStructType(Lists.newArrayList(venderId, venderId2, userId));
    }

    private static List<Row> initDatauseroinid() {
        List<Row> data = new ArrayList<Row>();
        for (int i = 0; i < 6000; i++) {
            if (i % 250 != 0) {
                data.add(RowFactory.create("user_9_" + i, "32", i + ""));
            }
            if (i % 300 != 0) {
                data.add(RowFactory.create("user_8_" + i, "8", i + ""));
            }
            if (i % 400 != 0) {
                data.add(RowFactory.create("user_10_" + i, "64", i + ""));
            }

        }
        return data;
    }

    private static String parseUserIdTable(String userIdField) {
        Matcher m = DB_TABLE_SPLIT.matcher(userIdField);
        String userIdTable = userIdField.split("\\.")[0];
        if (m.find()) {
            userIdTable = m.group(BigInteger.ONE.intValue());
        } else {
            throw new JobInterruptedException("userid解析异常");
        }
        return userIdTable;
    }
}

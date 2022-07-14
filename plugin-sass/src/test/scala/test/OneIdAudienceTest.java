package test;

import com.google.common.collect.Lists;
import com.jd.easy.audience.common.error.SparkApplicationErrorResult;
import com.jd.easy.audience.common.exception.JobInterruptedException;
import com.jd.easy.audience.common.exception.JobNeedReTryException;
import com.jd.easy.audience.common.util.JsonUtil;
import com.jd.easy.audience.task.commonbean.bean.AudienceGenerateBean;
import com.jd.easy.audience.task.commonbean.segment.SparkStepSegment;
import com.jd.easy.audience.task.driven.segment.Segment;
import com.jd.easy.audience.task.driven.segment.SegmentFactory;
import com.jd.easy.audience.task.driven.step.StepCommonBean;
import com.jd.easy.audience.task.plugin.step.audience.AudienceGenerateStep;
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
public class OneIdAudienceTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(OneIdAudienceTest.class);
    private static final Pattern DB_TABLE_SPLIT = Pattern.compile("^[a-zA-Z0-9|_]{1,10}\\.([_|\\w]*){1}(\\.[_|\\w]*)");

    public static void main(String[] args) {
        SparkSession sparkSession = new SparkSession.Builder()
                .master("local")
                .appName("OneIdUddTest")
                .getOrCreate();
        SparkContext sc = sparkSession.sparkContext();
        sc.setLogLevel("ERROR");
        mockData(sparkSession);
        /**
         * audience.json
         */
        SparkStepSegment sparkSegment = getsparkstepInfo("../audience/audience.json");
        try {
            //4.任务执行
            String stepName = sparkSegment.getBeans().keySet().toArray()[0].toString();
            StepCommonBean stepBean = sparkSegment.getBeans().get(stepName);
            Class c = Class.forName(stepBean.getStepClassName()); //baoming 是该类所在的包   leiming 是一个该类的名称的String
            LOGGER.info("bean class:" + stepBean.getStepClassName());
            AudienceGenerateStep stepObj = (AudienceGenerateStep) c.newInstance();
            stepObj.setStepBean(stepBean);
            stepObj.validate();
            Dataset<Row> data = stepObj.generateSql(sparkSession, (AudienceGenerateBean) stepBean, true);
            data.printSchema();
            data.show();
        } catch (Exception e) {
            //5.异常统一处理
            dealError(e);
        }

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
            System.out.println(OneIdAudienceTest.class.getResource("").getPath());
            File file = new File(OneIdAudienceTest.class.getResource("").getPath() + argPath);
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
        sparkSession.createDataFrame(initDatalayout(), initSchemalayout()).createOrReplaceTempView("4astatusmockdata_361");
        sparkSession.createDataFrame(initDatauseraudience(), initSchemauseraudience()).createOrReplaceTempView("audiencepackage_1647851765195823");
        sparkSession.createDataFrame(initDatauseroinid(), initSchemauseroneid()).createOrReplaceTempView("cdp_one_id_test");
        sparkSession.sql("select * from 4astatusmockdata_361").show();
        sparkSession.sql("select * from audiencepackage_1647851765195823").show();
        sparkSession.sql("select * from cdp_one_id_test").show();
    }

    private static StructType initSchemalayout() {
        StructField id = DataTypes.createStructField("user_id", DataTypes.StringType, true);
        StructField id2 = DataTypes.createStructField("user_id2", DataTypes.StringType, true);
        StructField venderId = DataTypes.createStructField("status", DataTypes.StringType, true);
        StructField dt = DataTypes.createStructField("dt", DataTypes.StringType, true);
        return DataTypes.createStructType(Lists.newArrayList(id, id2, venderId, dt));
    }

    private static List<Row> initDatalayout() {
        List<Row> data = new ArrayList<Row>();
        data.add(RowFactory.create("user_8_1", "", "1", "2022-03-17"));
        data.add(RowFactory.create("user_8_2", "user_9_2", "3", "2022-03-17"));
        data.add(RowFactory.create("", "user_9_3", "3", "2022-03-17"));
        data.add(RowFactory.create("user_8_4", "user_9_4", "3", "2022-03-17"));

        data.add(RowFactory.create("user_8_1", "", "1", "2022-02-16"));
        data.add(RowFactory.create("user_8_2", "user_9_2", "3", "2022-02-16"));
        data.add(RowFactory.create("user_8_11", "", "1", "2022-02-16"));
        data.add(RowFactory.create("user_8_10", "", "1", "2022-02-16"));
        data.add(RowFactory.create("user_8_22", "user_9_2", "3", "2022-02-16"));
        data.add(RowFactory.create("", "user_9_3", "3", "2022-02-16"));
        data.add(RowFactory.create("user_8_33", "user_9_4", "3", "2022-02-16"));
        return data;
    }

    private static StructType initSchemauseraudience() { //        StructField id = DataTypes.createStructField("id", DataTypes.IntegerType, true);
        StructField venderId = DataTypes.createStructField("user_id", DataTypes.StringType, true);
        return DataTypes.createStructType(Lists.newArrayList(venderId));
    }

    private static List<Row> initDatauseraudience() {
        List<Row> data = new ArrayList<Row>();
        data.add(RowFactory.create("oneid_1"));
        data.add(RowFactory.create("oneid_2"));
        data.add(RowFactory.create("oneid_3"));
        data.add(RowFactory.create("oneid_4"));
        data.add(RowFactory.create("oneid_5"));
        data.add(RowFactory.create("oneid_6"));
        data.add(RowFactory.create("oneid_10"));
        data.add(RowFactory.create("oneid_11"));

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
        data.add(RowFactory.create("user_9_1", "9", "oneid_1"));
        data.add(RowFactory.create("user_9_2", "9", "oneid_2"));
        data.add(RowFactory.create("user_9_3", "9", "oneid_3"));
        data.add(RowFactory.create("user_9_4", "9", "oneid_4"));
        data.add(RowFactory.create("user_9_5", "9", "oneid_5"));
        data.add(RowFactory.create("user_9_11", "9", "oneid_11"));
        data.add(RowFactory.create("user_8_1", "8", "oneid_1"));
        data.add(RowFactory.create("user_8_2", "8", "oneid_2"));
        data.add(RowFactory.create("user_8_4", "8", "oneid_3"));
        data.add(RowFactory.create("user_8_5", "8", "oneid_5"));
        data.add(RowFactory.create("user_8_7", "8", "oneid_7"));
        data.add(RowFactory.create("user_8_8", "8", "oneid_8"));
        data.add(RowFactory.create("user_8_9", "8", "oneid_9"));
        data.add(RowFactory.create("user_8_10", "8", "oneid_10"));
        data.add(RowFactory.create("user_8_11", "8", "oneid_11"));
        data.add(RowFactory.create("user_8_22", "8", "oneid_22"));
        data.add(RowFactory.create("user_8_33", "8", "oneid_33"));

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

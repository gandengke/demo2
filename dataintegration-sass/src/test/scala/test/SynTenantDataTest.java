package test;

import com.google.common.collect.Lists;
import com.jd.easy.audience.common.error.SparkApplicationErrorResult;
import com.jd.easy.audience.common.exception.JobInterruptedException;
import com.jd.easy.audience.common.exception.JobNeedReTryException;
import com.jd.easy.audience.common.util.JsonUtil;
import com.jd.easy.audience.task.commonbean.bean.SynData2TenantBean;
import com.jd.easy.audience.task.commonbean.segment.SparkStepSegment;
import com.jd.easy.audience.task.dataintegration.step.SynData2TenantStep;
import com.jd.easy.audience.task.dataintegration.util.CustomSerializeUtils;
import com.jd.easy.audience.task.driven.segment.Segment;
import com.jd.easy.audience.task.driven.segment.SegmentFactory;
import com.jd.easy.audience.task.driven.step.StepCommonBean;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author cdxiongmei
 * @version 1.0
 * @description 拆分数据测试
 * @date 2022/5/31 1:52 PM
 */
public class SynTenantDataTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SynTenantDataTest.class);

    public static void main(String[] args) {
        SparkSession sparkSession = new SparkSession.Builder()
                .master("local")
                .appName("SynTenantDataTest")
                .getOrCreate();
        SparkContext sc = sparkSession.sparkContext();
        sc.setLogLevel("ERROR");
        mockData(sparkSession);
        /**
         * load2tenantday.json 全域监测数据同步
         * loadmoduledata2tenant.json 模版数据测试
         * loadCrm2tenantday.json crm数据同步
         *
         */
        SparkStepSegment sparkSegment = getsparkstepInfo("../syndata/loadCrm2tenantday.json");
        try {
            //4.任务执行
            Object[] steps = sparkSegment.getBeans().keySet().toArray();
            for (int i = 0; i < steps.length; i++) {
                String stepName = steps[i].toString();
                StepCommonBean stepBean = sparkSegment.getBeans().get(stepName);
                Class c = Class.forName(stepBean.getStepClassName()); //baoming 是该类所在的包   leiming 是一个该类的名称的String
                LOGGER.info("bean class:" + stepBean.getStepClassName());
                SynData2TenantStep stepObj = (SynData2TenantStep) c.newInstance();
                stepObj.setStepBean(stepBean);
                stepObj.validate();
                stepObj.dealDataFilter(sparkSession, (SynData2TenantBean) stepBean, true);
            }
        } catch (Exception e) {
            //5.异常统一处理
            dealError(e);
        }
    }

    private static void dealError(Exception e) {
        SparkApplicationErrorResult errorResult = new SparkApplicationErrorResult();
        errorResult.setErrorClass(e.getClass().toString());
        errorResult.setErrorMsg(e.getMessage());
        errorResult.setStackTraceJson(CustomSerializeUtils.serialize(e));
        if (e.getCause() instanceof JobNeedReTryException) {
            JobNeedReTryException exception = (JobNeedReTryException) e;
            errorResult.setErrorEnMsg(exception.getMessageEn());
        } else if (e.getCause() instanceof JobInterruptedException) {
            JobInterruptedException exception = (JobInterruptedException) e;
            errorResult.setErrorEnMsg(exception.getMessageEn());
        } else {
            errorResult.setErrorEnMsg("Error: System Error!");
        }
        LOGGER.error("error orgin:" + errorResult.getErrorMsg());
        LOGGER.error("errorResult string=" + CustomSerializeUtils.serialize(errorResult));
    }

    private static SparkStepSegment getsparkstepInfo(String argPath) {
        SparkStepSegment sparkSegment = null;
        String segment = "";
        try {
            //2.1
            System.out.println(SynTenantDataTest.class.getResource("").getPath());
            File file = new File(SynTenantDataTest.class.getResource("").getPath() + argPath);
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
        sparkSession.createDataFrame(initDataClick(), initSchemaClick()).createOrReplaceTempView("cdp_base_click_full");
        sparkSession.createDataFrame(initDataImpression(), initSchemaImpression()).createOrReplaceTempView("cdp_base_impression_full");
        sparkSession.createDataFrame(initDatamodule(), initSchemamodule()).createOrReplaceTempView("clicklog");
        sparkSession.createDataFrame(initDatacrmmodule(), initSchemacrmmodule()).createOrReplaceTempView("adm_cdpsplitdata_crm_4a");
//        sparkSession.sql("select * from cdp_base_click_full").show();
//        sparkSession.sql("select * from cdp_base_impression_full").show();
//        sparkSession.sql("select * from clicklog").show();
        sparkSession.sql("select * from adm_cdpsplitdata_crm_4a").show();
    }

    private static StructType initSchemaClick() { //        StructField id = DataTypes.createStructField("id", DataTypes.IntegerType, true);
        StructField adPlanId = DataTypes.createStructField("ad_plan_id", DataTypes.StringType, true);
        StructField advertiserId = DataTypes.createStructField("advertiser_id", DataTypes.StringType, true);
        StructField userId = DataTypes.createStructField("user_id_click", DataTypes.StringType, true);
        StructField cdpUserId = DataTypes.createStructField("cdp_user_id", DataTypes.StringType, true);
        StructField cdpAccountId = DataTypes.createStructField("cdp_account_id", DataTypes.StringType, true);
        StructField pt = DataTypes.createStructField("pt", DataTypes.StringType, true);
        StructField dt = DataTypes.createStructField("dt", DataTypes.StringType, true);
        return DataTypes.createStructType(Lists.newArrayList(adPlanId, advertiserId, userId, cdpUserId, cdpAccountId, pt, dt));
    }

    private static List<Row> initDataClick() {
        List<Row> data = new ArrayList<Row>();
        data.add(RowFactory.create("2496456003944448", "", "1281945641", "10000138", "10042", "ca", "2022-05-30"));
        data.add(RowFactory.create("2496455952564225", "", "1281945641", "10000138", "10042", "ca", "2022-05-30"));
        data.add(RowFactory.create("2495933136764928", "", "1281945641", "10000138", "10042", "ca", "2022-05-30"));
        data.add(RowFactory.create("2495948298125312", "", "119285607", "10000138", "10042", "ca", "2022-05-30"));
        data.add(RowFactory.create("2496456003944448", "", "1281945641", "10000138", "10042", "ca", "2022-05-30"));
        data.add(RowFactory.create("2496455880212481", "", "1281945641", "10000138", "10042", "ca", "2022-05-30"));
        data.add(RowFactory.create("2495933136764928", "", "1281945641", "10000138", "10042", "ca", "2022-05-30"));
        data.add(RowFactory.create("2496454793887745", "", "1281945641", "10000138", "10042", "ca", "2022-05-30"));
        data.add(RowFactory.create("", "475180", "40", "10000138", "10042", "tpm", "2022-05-30"));
        data.add(RowFactory.create("", "475180", "42", "10000138", "10042", "tpm", "2022-05-30"));
        data.add(RowFactory.create("", "475180", "41", "10000138", "10042", "tpm", "2022-05-30"));
        return data;
    }

    private static StructType initSchemaImpression() {
        StructField adPlanId = DataTypes.createStructField("ad_plan_id", DataTypes.StringType, true);
        StructField advertiserId = DataTypes.createStructField("advertiser_id", DataTypes.StringType, true);
        StructField userId = DataTypes.createStructField("user_id_impression", DataTypes.StringType, true);
        StructField cdpUserId = DataTypes.createStructField("cdp_user_id", DataTypes.StringType, true);
        StructField cdpAccountId = DataTypes.createStructField("cdp_account_id", DataTypes.StringType, true);
        StructField pt = DataTypes.createStructField("pt", DataTypes.StringType, true);
        StructField dt = DataTypes.createStructField("dt", DataTypes.StringType, true);
        return DataTypes.createStructType(Lists.newArrayList(adPlanId, advertiserId, userId, cdpUserId, cdpAccountId, pt, dt));
    }

    private static List<Row> initDataImpression() {
        List<Row> data = new ArrayList<Row>();
        data.add(RowFactory.create("2495933136764928", "", "1281945641", "10000138", "10042", "ca", "2022-05-30"));
        data.add(RowFactory.create("2495933136764928", "", "119285607", "10000138", "10042", "ca", "2022-05-30"));
        data.add(RowFactory.create("2495321406963713", "", "1281945641", "10000138", "10042", "ca", "2022-05-30"));
        data.add(RowFactory.create("2495948298125312", "", "1281945641", "10000138", "10042", "ca", "2022-05-30"));
        data.add(RowFactory.create("2495952367648769", "", "1281945641", "10000138", "10042", "ca", "2022-05-30"));
        data.add(RowFactory.create("2495949801783296", "", "1281945641", "10000138", "10042", "ca", "2022-05-30"));
        data.add(RowFactory.create("", "475180", "50", "10000138", "10042", "tpm", "2022-05-30"));
        data.add(RowFactory.create("", "475180", "52", "10000138", "10042", "tpm", "2022-05-30"));
        data.add(RowFactory.create("", "475180", "51", "10000138", "10042", "tpm", "2022-05-30"));
        return data;
    }

    private static StructType initSchemamodule() { //        StructField id = DataTypes.createStructField("id", DataTypes.IntegerType, true);
        StructField idfa = DataTypes.createStructField("idfa", DataTypes.StringType, true);
        StructField userId = DataTypes.createStructField("user_id", DataTypes.StringType, true);

        StructField accountId = DataTypes.createStructField("account_id", DataTypes.StringType, true);
        StructField sourceId = DataTypes.createStructField("source_id", DataTypes.StringType, true);
        StructField eventDate = DataTypes.createStructField("event_date", DataTypes.StringType, true);
        return DataTypes.createStructType(Lists.newArrayList(idfa, userId, accountId, sourceId, eventDate));
    }

    private static List<Row> initDatamodule() {
        List<Row> data = new ArrayList<Row>();
        data.add(RowFactory.create("user_9_1", "10008327", "11759", "122352", "2022-05-31"));
        data.add(RowFactory.create("user_9_2", "10008327", "11759", "122352", "2022-05-31"));
        data.add(RowFactory.create("user_9_3", "10008327", "11759", "122352", "2022-05-31"));
        data.add(RowFactory.create("user_9_4", "10008327", "11759", "122352", "2022-05-31"));
        data.add(RowFactory.create("user_9_5", "10008327", "11759", "122352", "2022-05-31"));
        data.add(RowFactory.create("user_9_15", "10008327", "11759", "122352", "2022-05-31"));
        data.add(RowFactory.create("user_9_16", "10008327", "11759", "122352", "2022-05-31"));
        data.add(RowFactory.create("user_9_17", "10008327", "11759", "122352", "2022-05-31"));
        return data;
    }

    private static StructType initSchemacrmmodule() {
        StructField idfa = DataTypes.createStructField("idfa", DataTypes.StringType, true);
        StructField userId = DataTypes.createStructField("user_id", DataTypes.StringType, true);

        StructField dayNoShop = DataTypes.createStructField("day_no_shop", DataTypes.StringType, true);
        StructField latestOrderDate = DataTypes.createStructField("latest_order_date", DataTypes.StringType, true);
        StructField dt = DataTypes.createStructField("dt", DataTypes.StringType, true);
        StructField accountId = DataTypes.createStructField("account_id", DataTypes.StringType, true);
        return DataTypes.createStructType(Lists.newArrayList(idfa, userId, dayNoShop, latestOrderDate, dt, accountId));
    }

    private static List<Row> initDatacrmmodule() {
        List<Row> data = new ArrayList<Row>();
        data.add(RowFactory.create("user_9_1", "10008327", "11759", "122352", "2022-05-31", "11944"));
        data.add(RowFactory.create("user_9_2", "10008327", "11759", "122352", "2022-05-31", "11944"));
        data.add(RowFactory.create("user_9_3", "10008327", "11759", "122352", "2022-05-31", "11944"));
        data.add(RowFactory.create("user_9_4", "10008327", "11759", "122352", "2022-05-31", "11944"));
        data.add(RowFactory.create("user_9_5", "10008327", "11759", "122352", "2022-05-31", "11944"));
        data.add(RowFactory.create("user_9_15", "10008327", "11759", "122352", "2022-05-31", "11944"));
        data.add(RowFactory.create("user_9_16", "10008327", "11759", "122352", "2022-05-31", "11944"));
        data.add(RowFactory.create("user_9_17", "10008327", "11759", "122352", "2022-05-31", "11944"));
        return data;
    }
}

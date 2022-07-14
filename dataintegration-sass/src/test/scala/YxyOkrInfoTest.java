import com.google.common.collect.Lists;
import com.jd.easy.audience.task.dataintegration.run.spark.YxyOkrTableInfoUpdate;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author cdxiongmei
 * @version 1.0
 * @description 拆分数据测试
 * @date 2022/5/31 1:52 PM
 */
public class YxyOkrInfoTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(YxyOkrInfoTest.class);

    public static void main(String[] args) {
        SparkSession sparkSession = new SparkSession.Builder()
                .master("local")
                .appName("YxyOkrInfoTest")
                .getOrCreate();
        SparkContext sc = sparkSession.sparkContext();
        sc.setLogLevel("ERROR");
        mockData(sparkSession);
        DateTime jobTime = DateTime.parse("20220617010000", DateTimeFormat.forPattern("yyyyMMddHHmmss"));
        YxyOkrTableInfoUpdate.dealOkrData(sparkSession, jobTime, true);
    }

    private static void mockData(SparkSession sparkSession) {
        sparkSession.createDataFrame(initDataClick(), initSchemaClick()).createOrReplaceTempView("adm_yxy_okr_table_info_tb");
//        sparkSession.sql("select * from clicklog").show();
        sparkSession.sql("select * from adm_cdpsplitdata_crm_4a").show();
    }

    private static StructType initSchemaClick() { //        StructField id = DataTypes.createStructField("id", DataTypes.IntegerType, true);
        StructField adPlanId = DataTypes.createStructField("rows", DataTypes.LongType, true);
        StructField advertiserId = DataTypes.createStructField("table_name", DataTypes.StringType, true);
        StructField userId = DataTypes.createStructField("source_type", DataTypes.StringType, true);
        StructField cdpUserId = DataTypes.createStructField("table_create_time", DataTypes.StringType, true);
        StructField cdpAccountId = DataTypes.createStructField("table_update_time", DataTypes.StringType, true);
        StructField pt = DataTypes.createStructField("dt", DataTypes.StringType, true);
        StructField dt = DataTypes.createStructField("account_id", DataTypes.StringType, true);
        return DataTypes.createStructType(Lists.newArrayList(adPlanId, advertiserId, userId, cdpUserId, cdpAccountId, pt, dt));
    }

    private static List<Row> initDataClick() {
        List<Row> data = new ArrayList<Row>();
        data.add(RowFactory.create(30L, "", "adm_clicklog_10300_33456_1314", "2", "2022-06-10 12:00:00", "2022-06-10 12:00:00", "2022-06-10", "10300"));
        data.add(RowFactory.create(40L, "", "adm_impresslog_10300_33456_1313", "3", "2022-06-10 12:00:00", "2022-06-10 12:00:00", "2022-06-10", "10300"));
        data.add(RowFactory.create(50L, "", "adm_clicklog_10300_33456_1316", "2", "2022-06-06 12:00:00", "2022-06-10 12:00:00", "2022-06-10", "10300"));
        data.add(RowFactory.create(60L, "", "adm_impresslog_10300_33456_1315", "2", "2022-06-10 12:00:00", "2022-06-10 12:00:00", "2022-06-10", "10300"));
        data.add(RowFactory.create(70L, "", "adm_custometable", "1", "2022-06-10 12:00:00", "2022-06-10 12:00:00", "2022-06-10", "10300"));

        data.add(RowFactory.create(50L, "", "adm_clicklog_10300_33456_1314", "2", "2022-06-10 12:00:00", "2022-06-10 12:00:00", "2022-06-16", "10300"));
        data.add(RowFactory.create(42L, "", "adm_impresslog_10300_33456_1313", "3", "2022-06-10 12:00:00", "2022-06-10 12:00:00", "2022-06-16", "10300"));
        data.add(RowFactory.create(53L, "", "adm_clicklog_10300_33456_1316", "2", "2022-06-06 12:00:00", "2022-06-10 12:00:00", "2022-06-16", "10300"));
        data.add(RowFactory.create(77L, "", "adm_custometable", "1", "2022-06-10 12:00:00", "2022-06-10 12:00:00", "2022-06-16", "10300"));
        data.add(RowFactory.create(20L, "", "adm_custometable2", "1", "2022-06-10 12:00:00", "2022-06-10 12:00:00", "2022-06-16", "10300"));
        data.add(RowFactory.create(24L, "", "adm_modelengine", "8", "2022-06-10 12:00:00", "2022-06-10 12:00:00", "2022-06-16", "10300"));

        data.add(RowFactory.create(2L, "", "adm_clicklog_10300_33456_1314", "2", "2022-06-10 12:00:00", "2022-06-10 12:00:00", "2021-12-31", "10300"));
        data.add(RowFactory.create(20L, "", "adm_custometable2", "1", "2022-06-10 12:00:00", "2022-06-10 12:00:00", "2021-12-31", "10300"));
        data.add(RowFactory.create(10L, "", "adm_modelengine", "8", "2022-06-10 12:00:00", "2022-06-10 12:00:00", "2021-12-31", "10300"));

        data.add(RowFactory.create(2L, "", "adm_clicklog_10300_33456_1314", "2", "2022-06-10 12:00:00", "2022-06-10 12:00:00", "2021-12-31", "10300"));
        data.add(RowFactory.create(2L, "", "adm_custometable2", "1", "2022-06-10 12:00:00", "2022-06-10 12:00:00", "2021-12-31", "10300"));
        data.add(RowFactory.create(1L, "", "adm_modelengine", "8", "2022-06-10 12:00:00", "2022-06-10 12:00:00", "2021-12-31", "10300"));
        data.add(RowFactory.create(10L, "", "adm_modelengine4", "8", "2022-06-10 12:00:00", "2022-06-10 12:00:00", "2021-12-31", "10300"));
        return data;
    }
}

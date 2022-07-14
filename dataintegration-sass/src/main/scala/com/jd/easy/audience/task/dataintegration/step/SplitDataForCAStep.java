package com.jd.easy.audience.task.dataintegration.step;

import com.jd.easy.audience.common.constant.SplitDataSourceEnum;
import com.jd.easy.audience.common.constant.SplitDataTypeEnum;
import com.jd.easy.audience.common.exception.JobInterruptedException;
import com.jd.easy.audience.common.util.JsonUtil;
import com.jd.easy.audience.task.commonbean.bean.SplitDataBean;
import com.jd.easy.audience.task.commonbean.contant.CATemplateEnum;
import com.jd.easy.audience.task.commonbean.contant.SplitSourceTypeEnum;
import com.jd.easy.audience.task.dataintegration.property.ConfigProperties;
import com.jd.easy.audience.task.dataintegration.util.SparkUtil;
import com.jd.easy.audience.task.driven.run.StepExecutor;
import com.jd.easy.audience.task.driven.step.StepCommonBean;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Author: cdxiongmei
 * @Date: 2021/7/9 上午10:51
 * @Description: ca数据拆分的前置处理
 */
@Deprecated
public class SplitDataForCAStep {
    private static final Logger LOGGER = LoggerFactory.getLogger(SplitDataForCAStep.class);

    public static void main(String[] args) throws Exception {
        LOGGER.info("参数个数" + args.length);

        SparkSession sparkSession = SparkUtil.buildMergeFileSession("SplitDataForCA");
        //查日志表，过滤出最新更新日志
        String statDate = args[0];
        String startMin = args[1];
        String endMin = args[2];
        String all = args[3]; //是否所有历史默认为false
        String isRebuild = args[4]; //是否需要重建拆分表
        String moduleName = args[5]; //模版名称
        String accountIdFilter = args[6]; //模版名称
        LOGGER.info("参数statDate={},startMin={},endMin={}", statDate, startMin, endMin);
        String markTable = ConfigProperties.MARK_TABLE();
        String filterCondition = "\t\t\tdt = '" + statDate + "'\n" +
                "\t\t\tand tm = '" + startMin + "'\n" +
                "\t\t\tand status =0 \n" +
                "\t\t\tand event_type !=3";
        if (Boolean.TRUE.toString().equalsIgnoreCase(all)) {
            filterCondition = "\t\t\tstatus =0 \n" +
                    "\t\t\tand event_type !=3";
        }
        if (!"all".equalsIgnoreCase(moduleName)) {
            filterCondition += "\t\t\tand template_id =\"" + moduleName + "\"";
        }
        if (!"all".equalsIgnoreCase(accountIdFilter)) {
            filterCondition += "\t\t\tand account_id =\"" + accountIdFilter + "\"";
        }
        String markLog = "SELECT\n" +
                "\tsource_type,\n" +
                "\tsource_name,\n" +
                "\ttemplate_id,\n" +
                "\taccount_id    ,\n" +
                "\tuser_id   as tenant_id,\n" +
                "\tsource_id  ,\n" +
                "\tevent_time\n" +
                "FROM\n" +
                "\t(\n" +
                "\t\tSELECT\n" +
                "\t\t\tsource_type,\n" +
                "\t\t\tsource_name,\n" +
                "\t\t\ttemplate_id,\n" +
                "\t\t\taccount_id    ,\n" +
                "\t\t\tuser_id    ,\n" +
                "\t\t\tsource_id  ,\n" +
                "\t\t\tevent_time ,\n" +
                "\t\t\trank() over(partition BY template_id, user_id, source_id order by event_time DESC) AS rank\n" +
                "\t\tFROM\n" + markTable +
                "\t\tWHERE\n" + filterCondition +
                ")a\n" +
                "WHERE\n" +
                "\trank = 1";
        LOGGER.info("mark log sql:" + markLog);
        List<Row> markRows = sparkSession.sql(markLog).collectAsList();
        LOGGER.info("mark rows:{}", markRows.size());
        sparkSession.close();
        Map<String, StepCommonBean> beans = new HashMap<>();
        for (Row mark : markRows) {
            //通过日志查询需要拆分的数据
            String templateName = mark.getAs("template_id").toString();
            String tenantId = mark.getAs("tenant_id").toString();
            String accountId = mark.getAs("account_id").toString();
            String sourceId = mark.getAs("source_id").toString();
            String sourceType = mark.getAs("source_type").toString();

            LOGGER.info("templateName={}, accountId={},tenantId={}, sourceId={}, sourceType={}", templateName, accountId, tenantId, sourceId, sourceType);
            CATemplateEnum templateEnum = CATemplateEnum.getCATemplateEnum(templateName);
            if (null == templateEnum) {
                LOGGER.error("暂时不支持该模版templateName={}", templateName);
                continue;
            }
            SplitDataBean splitBean = new SplitDataBean();
            splitBean.setSourceTable(templateEnum.getTemplateTable());
            splitBean.setSourceDbName(ConfigProperties.MODULE_DB_NAME());
            splitBean.setDataSource(SplitDataSourceEnum.CA);
            SplitSourceTypeEnum typeEnum = SplitSourceTypeEnum.getTypeEnum(sourceType);
            splitBean.setSourceTypeEx(typeEnum);
            if (templateEnum.isPartition()) {
                //拆分成分区表
                splitBean.setDataType(SplitDataTypeEnum.NONEPARTITIONED_FULLSCALE_ALLUPDATE_PARTITIONED);
                splitBean.setDtFieldSource(templateEnum.getPartitionField());
            } else {
                //拆分成非分区表
                splitBean.setDataType(SplitDataTypeEnum.NONEPARTITIONED_FULLSCALE_TO_NONEPARTITIONED);
            }
            splitBean.setDesTablePre("adm_" + templateName + "_" + sourceId);
            splitBean.setTenantFieldSource("account_id,user_id");
            splitBean.setTableComment(mark.getAs("source_name").toString());
            splitBean.setFilterExpress(" (source_id= \"" + sourceId + "\" and user_id=\"" + tenantId + "\" and account_id=\"" + accountId + "\")");
            String stepName = "SplitDataForCAStep_" + templateName + "_" + accountId + "_" + tenantId + "_" + sourceId;
            splitBean.setStepName(stepName);
            splitBean.setRebuilt(false);
            if (Boolean.TRUE.toString().equalsIgnoreCase(isRebuild)) {
                splitBean.setRebuilt(true);
            }
            splitBean.setStepClassName("com.jd.easy.audience.task.dataintegration.step.SplitDataStep");
            beans.put(stepName, splitBean);
            //拆分任务调用
            LOGGER.info("beans:" + JsonUtil.serialize(beans));

        }
        try {
            //3.任务执行
            StepExecutor.run("CA_SplitStep", beans);
        } catch (Exception ex) {
            LOGGER.error("ca split exception", ex);
            throw new JobInterruptedException("ca 模板数据拆分异常", "ca module data split exception");
        } finally {
            //6.清理现场
            StepExecutor.shutdownThreadPool();
        }
        LOGGER.info("SplitDataForCAStep finished");
    }

    /**
     * @throws
     * @title getDateStrBeforeNDays
     * @description 加工前n天的日期
     * @author cdxiongmei
     * @param: date
     * @param: interval
     * @updateTime 2021/8/5 上午11:45
     * @return: java.lang.String
     */
    private static String getDateStrBeforeNDays(Date date, int interval) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DATE, -interval);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormat.format(cal.getTime());
    }

    /**
     * @throws
     * @title getTmFilter
     * @description 加工周期运行的时间范围
     * @author cdxiongmei
     * @updateTime 2021/8/5 上午11:44
     * @return: java.util.Map<java.lang.String, java.lang.String>
     */
    private static Map<String, String> getTmFilter() {
        Date now = new Date();
        Calendar calMax = Calendar.getInstance();
        calMax.setTime(now);
        Calendar calMin = Calendar.getInstance();
        calMin.setTime(now);
        int mod = 30;
        System.out.println(now.getHours());
        System.out.println(now.getMinutes());
        int modMin = Math.floorMod(now.getMinutes(), mod);
        calMax.add(Calendar.MINUTE, -modMin);
        calMin.add(Calendar.MINUTE, -modMin - mod);
        Date max = calMax.getTime();
        Date min = calMin.getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("HHmm");
        System.out.println("max:" + sdf.format(calMax.getTime()));
        System.out.println("min:" + sdf.format(calMin.getTime()));
        Map<String, String> mapTm = new HashMap<>();
        mapTm.put("max", sdf.format(calMax.getTime()));
        mapTm.put("min", sdf.format(calMin.getTime()));
        return mapTm;
    }

}

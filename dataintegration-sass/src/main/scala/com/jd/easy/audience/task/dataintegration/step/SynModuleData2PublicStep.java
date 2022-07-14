package com.jd.easy.audience.task.dataintegration.step;

import com.jd.easy.audience.common.constant.SplitDataSourceEnum;
import com.jd.easy.audience.common.constant.SplitDataTypeEnum;
import com.jd.easy.audience.common.exception.JobInterruptedException;
import com.jd.easy.audience.common.util.JsonUtil;
import com.jd.easy.audience.task.commonbean.bean.SynData2PublicBean;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: cdxiongmei
 * @Date: 2021/7/9 上午10:51
 * @Description: ca数据拆分的前置处理
 */
@Deprecated
public class SynModuleData2PublicStep {
    private static final Logger LOGGER = LoggerFactory.getLogger(SynModuleData2PublicStep.class);

    public static void main(String[] args) {
        LOGGER.info("参数个数" + args.length);

        SparkSession sparkSession = SparkUtil.buildMergeFileSession("SynModuleData2PublicStep");
        //查日志表，过滤出最新更新日志
        String statDate = args[0];
        String startMin = args[1];
        String endMin = args[2];
        String all = args[3]; //是否所有历史默认为false
        String isRebuild = args[4]; //是否需要重建拆分表
        String moduleName = args[5]; //模版名称
        String accountIdFilter = args[6]; //组织账号id
        LOGGER.info("参数statDate={},startMin={},endMin={}, all={}, isRebuild={}, moduleName={}, accountIdFilter={}", statDate, startMin, endMin, all, isRebuild, moduleName, accountIdFilter);
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
                "\t\t\trank() over(partition BY template_id,account_id, user_id, source_id order by event_time DESC) AS rank\n" +
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
        //需要把同一个模版的数据放在一起处理
        for (Row mark : markRows) {
            //通过日志查询需要拆分的数据
            String templateName = mark.getAs("template_id").toString();
            String tenantId = mark.getAs("tenant_id").toString();
            String accountId = mark.getAs("account_id").toString();
            String sourceId = mark.getAs("source_id").toString();
            String sourceType = mark.getAs("source_type").toString();
            String stepName = "SplitDataForCAStep_" + templateName;
            if (beans.containsKey(stepName)) {
                SynData2PublicBean beanHis = (SynData2PublicBean) beans.get(stepName);
                String filterExpress = "(" + beanHis.getFilterExpress() + " OR (source_id= \"" + sourceId + "\" and user_id=\"" + tenantId + "\" and account_id=\"" + accountId + "\"))";
                beanHis.setFilterExpress(filterExpress);
                beans.put(stepName, beanHis);
                continue;
            }
            LOGGER.info("templateName={}, accountId={},tenantId={}, sourceId={}, sourceType={}", templateName, accountId, tenantId, sourceId, sourceType);
            CATemplateEnum templateEnum = CATemplateEnum.getCATemplateEnum(templateName);
            if (null == templateEnum) {
                LOGGER.error("暂时不支持该模版templateName={}", templateName);
                continue;
            }
            SynData2PublicBean splitBean = new SynData2PublicBean();
            splitBean.setSourceTableName(templateEnum.getTemplateTable());
            splitBean.setSourceDbName(ConfigProperties.MODULE_DB_NAME());
            splitBean.setDesTableName("adm_cdpsplitdata_module_" + templateName);
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
            splitBean.setSubTableNamePrefix("adm_" + templateName + "_");
            splitBean.setTenantFieldSource("account_id,user_id");
            splitBean.setSplitFieldEx("source_id");
            splitBean.setTableComment(mark.getAs("source_name").toString());
            splitBean.setFilterExpress(" (source_id= \"" + sourceId + "\" and user_id=\"" + tenantId + "\" and account_id=\"" + accountId + "\")");

            splitBean.setStepName(stepName);
            splitBean.setIsRebuilt(false);
            if (Boolean.TRUE.toString().equalsIgnoreCase(isRebuild)) {
                splitBean.setIsRebuilt(true);
            }
            splitBean.setStepClassName("com.jd.easy.audience.task.dataintegration.step.SynData2PublicStep");
            beans.put(stepName, splitBean);
            //拆分任务调用
            LOGGER.info("beans:" + JsonUtil.serialize(beans));

        }
        LOGGER.info(beans.toString());
        try {
            //3.任务执行
            StepExecutor.run("CA_SplitStep", beans);
        } catch (Exception ex) {
            LOGGER.error("ca split exception", ex);
            throw new JobInterruptedException("ca 模板数据拆分异常", "ca module data split exception");
        } finally {
            //6.清理现场
            StepExecutor.shutdownThreadPool();
            sparkSession.close();
        }
        LOGGER.info("SplitDataForCAStep finished");
    }
}

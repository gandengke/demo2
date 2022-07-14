package com.jd.easy.audience.task.dataintegration.util.split.impl;

import com.jd.easy.audience.common.constant.NumberConstant;
import com.jd.easy.audience.common.constant.SplitDataSourceEnum;
import com.jd.easy.audience.common.constant.SplitDataTypeEnum;
import com.jd.easy.audience.common.exception.JobInterruptedException;
import com.jd.easy.audience.common.util.JsonUtil;
import com.jd.easy.audience.task.commonbean.bean.SynData2TenantBean;
import com.jd.easy.audience.task.commonbean.contant.CATemplateEnum;
import com.jd.easy.audience.task.commonbean.contant.SplitSourceTypeEnum;
import com.jd.easy.audience.task.commonbean.segment.SparkStepSegment;
import com.jd.easy.audience.task.dataintegration.property.ConfigProperties;
import com.jd.easy.audience.task.dataintegration.util.SparkUtil;
import com.jd.easy.audience.task.dataintegration.util.split.IBeansMapBuilding;
import com.jd.easy.audience.task.driven.step.StepCommonBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ca模版数据拆分
 *
 * @author cdxiongmei
 * @version V1.0
 * @Date 2021-12-27
 */
public class ModuleBeansMapBuilding implements IBeansMapBuilding {
    private static final Logger LOGGER = LoggerFactory.getLogger(ModuleBeansMapBuilding.class);

    @Override
    public Map<String, StepCommonBean> builtBeans(SparkStepSegment sparkSegment, String accountId, String accountName, String dbName) {
        Map<String, StepCommonBean> beans = new HashMap<String, StepCommonBean>(NumberConstant.INT_10);
        String buffaloNtime = System.getenv(ConfigProperties.BUFFALO_ENV_NTIME());
        DateTime jobTime = DateTime.parse(buffaloNtime, DateTimeFormat.forPattern("yyyyMMddHHmmss"));
        List<Row> markRows = getMarkInfo(jobTime, accountId);
        //从配置中获取指定的参数文件内容
        Map<String, StepCommonBean> beansMap = sparkSegment.getBeans();
        markRows.forEach(mark -> {
            //通过日志查询需要拆分的数据
            String templateName = mark.getAs("template_id").toString();
            String userId = mark.getAs("user_id").toString();
            String sourceId = mark.getAs("source_id").toString();
            String sourceType = mark.getAs("source_type").toString();
            String stepName = "SplitDataForCAStep_" + templateName + "_" + accountId + "_" + userId + "_" + sourceId;
            LOGGER.info("templateName={}, accountId={},userId={}, sourceId={}, sourceType={}", templateName, accountId, userId, sourceId, sourceType);
            CATemplateEnum templateEnum = CATemplateEnum.getCATemplateEnum(templateName);
            if (null == templateEnum) {
                LOGGER.error("暂时不支持该模版templateName={}", templateName);
            } else {
                SynData2TenantBean cirbean = (SynData2TenantBean) beansMap.get("loadModuleData4TenantBean_" + templateName);
                SynData2TenantBean subAccountbean = null;
                try {
                    subAccountbean = (SynData2TenantBean) BeanUtils.cloneBean(cirbean);
                } catch (Exception e) {
                    LOGGER.info("ModuleBeansMapBuilding occurs exception", e);
                    throw new JobInterruptedException("ModuleBeansMapBuilding occurs exception");
                }
                SplitSourceTypeEnum typeEnum = SplitSourceTypeEnum.getTypeEnum(sourceType);
                subAccountbean.setSourceTypeEx(typeEnum);
                if (templateEnum.isPartition()) { //拆分成分区表
                    subAccountbean.setDataType(SplitDataTypeEnum.NONEPARTITIONED_FULLSCALE_ALLUPDATE_PARTITIONED);
                    subAccountbean.setDtFieldSource(templateEnum.getPartitionField());
                } else { //拆分成非分区表
                    subAccountbean.setDataType(SplitDataTypeEnum.NONEPARTITIONED_FULLSCALE_TO_NONEPARTITIONED);
                }
                subAccountbean.setDataSource(SplitDataSourceEnum.CA);
                subAccountbean.setSubTableName("adm_" + cirbean.getSubTableName() + "_" + accountId + "_" + userId + "_" + sourceId);
                subAccountbean.setTableComment(mark.getAs("source_name").toString());
                subAccountbean.setFilterExpress("user_id= \"" + userId + "\" AND account_id=\"" + accountId + "\" AND source_id=\"" + sourceId + "\"");
                subAccountbean.setStepName(stepName);
                subAccountbean.setStepClassName("com.jd.easy.audience.task.dataintegration.step.SynData2TenantStep");
                subAccountbean.setAccountId(Long.valueOf(accountId));
                subAccountbean.setAccountName(accountName);
                subAccountbean.setDbName(dbName);
                subAccountbean.setSplitFieldEx("source_id");
                beans.put(stepName, subAccountbean);
                //拆分任务调用
                LOGGER.info("beans:" + JsonUtil.serialize(beans));
            }
        });
        return beans;
    }

    /**
     * 日志数据获取，通过日志记录封装要处理的数据
     *
     * @param jobTime
     * @param accountId
     * @return
     */
    private List<Row> getMarkInfo(DateTime jobTime, String accountId) {
        String markTable = ConfigProperties.MARK_TABLE();
        String statDate = jobTime.toString("yyyy-MM-dd");
        String startMin = jobTime.toString("HHmm");
        String filterCondition = "\t\t\tdt = '" + statDate + "'\n" +
                "\t\t\tand tm = '" + startMin + "'\n" +
                "\t\t\tand status =0 \n" +
                "\t\t\tand event_type !=3 \n" +
                "\t\t\tand account_id=\"" + accountId + "\"";
        String markLog = "SELECT\n" +
                "\tsource_type,\n" +
                "\tsource_name,\n" +
                "\ttemplate_id,\n" +
                "\taccount_id    ,\n" +
                "\tuser_id ,\n" +
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
                "WHERE\n" + "\trank = 1";
        LOGGER.info("mark log sql:" + markLog);
        SparkSession sparkSession = SparkUtil.buildMergeFileSession("ModuleBeansMapBuilding_" + accountId);
        List<Row> markRows = sparkSession.sql(markLog).collectAsList();
        LOGGER.info("mark rows:{}", markRows.size());
        sparkSession.close();
        return markRows;
    }

}

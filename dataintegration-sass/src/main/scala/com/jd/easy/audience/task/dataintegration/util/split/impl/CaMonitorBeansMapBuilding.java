package com.jd.easy.audience.task.dataintegration.util.split.impl;

import com.jd.easy.audience.common.constant.NumberConstant;
import com.jd.easy.audience.task.commonbean.bean.SynData2TenantBean;
import com.jd.easy.audience.task.commonbean.segment.SparkStepSegment;
import com.jd.easy.audience.task.dataintegration.property.ConfigProperties;
import com.jd.easy.audience.task.dataintegration.util.SynDataFilterExpressParserEnum;
import com.jd.easy.audience.task.dataintegration.util.split.IBeansMapBuilding;
import com.jd.easy.audience.task.driven.step.StepCommonBean;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * ca站点监控回流数据拆分
 *
 * @author cdxiongmei
 * @version V1.0
 */
public class CaMonitorBeansMapBuilding implements IBeansMapBuilding {
    private static final Logger LOGGER = LoggerFactory.getLogger(CaMonitorBeansMapBuilding.class);

    @Override
    public Map<String, StepCommonBean> builtBeans(SparkStepSegment sparkSegment, String accountId, String accountName, String dbName) throws IOException {
        String buffaloNtime = System.getenv(ConfigProperties.BUFFALO_ENV_NTIME());
        String proAccountName = System.getenv(ConfigProperties.HADOOP_USER()); //生产账号
        DateTime jobTime = DateTime.parse(buffaloNtime, DateTimeFormat.forPattern("yyyyMMddHHmmss"));
        Map<String, String> paramValue = new HashMap<>(NumberConstant.INT_10);
        paramValue.put(SynDataFilterExpressParserEnum.DAY.getCode(), jobTime.toString("yyyy-MM-dd"));
        paramValue.put(SynDataFilterExpressParserEnum.ACCOUNT_ID.getCode(), accountId);
        paramValue.put(SynDataFilterExpressParserEnum.BDP_PROD_ACCOUNT.getCode(), proAccountName);
        sparkSegment.getBeans().forEach((k, v) -> {
            SynData2TenantBean circlebean = (SynData2TenantBean) v;
            circlebean.setAccountId(Long.valueOf(accountId));
            circlebean.setAccountName(accountName);
            circlebean.setDbName(dbName);
            String filterExpress = SynDataFilterExpressParserEnum.parse(circlebean.getFilterExpress(), paramValue);
            circlebean.setFilterExpress(filterExpress);
        });
        return sparkSegment.getBeans();
    }

}

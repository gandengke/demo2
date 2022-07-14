package com.jd.easy.audience.task.dataintegration.util;

import com.google.common.collect.Maps;
import com.jd.easy.audience.common.constant.NumberConstant;
import com.jd.easy.audience.common.exception.JobInterruptedException;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
public enum SynDataFilterExpressParserEnum {

    DAY("DAY", "时间占位"),
    BDP_PROD_ACCOUNT("BDP_PROD_ACCOUNT", "BDP生产账号"),
    ACCOUNT_ID("ACCOUNT_ID", "jnos主账号id"),
    TENANT_ID("TENANT_ID", "crm商家字段");

    private String code;
    private String desc;

    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    private static Map<String, SynDataFilterExpressParserEnum> tmpMap = new HashMap<String, SynDataFilterExpressParserEnum>();

    static {
        for (SynDataFilterExpressParserEnum e : SynDataFilterExpressParserEnum.values()) {
            tmpMap.put(e.getCode(), e);
        }
    }

    /**
     * @throws
     * @Title: JobExpressParserEnum
     * @param:
     */
    private SynDataFilterExpressParserEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static SynDataFilterExpressParserEnum get(String code) {
        if (StringUtils.isBlank(code)) {
            return null;
        }
        return tmpMap.get(code);
    }

    private static final Pattern dayPattern = Pattern.compile("\\{\\{DAY(.*?)\\}\\}");

    /**
     * 占位符处理处理
     *
     * @param value
     * @return
     */
    public static String parse(String value, Map<String, String> params) {
        String newValue = value;
        for (String parseKey : params.keySet()) {
            if (parseKey.equalsIgnoreCase(DAY.code)) {
                newValue = dayExpressParse(newValue, params.get(DAY.code));
            } else if (newValue.indexOf(parseKey) >= NumberConstant.INT_0) {
                newValue = newValue.replace("{{" + parseKey + "}}", params.get(parseKey));
            }
        }
        return newValue;
    }

    private static String getDateNextNDays(Date dt, int interval) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        cal.setTime(dt);
        cal.add(Calendar.DATE, interval);
        return dateFormat.format(cal.getTime());
    }

    /**
     * 时间占位处理
     *
     * @param expressStr
     * @param dateStr
     * @return
     */
    private static String dayExpressParse(String expressStr, String dateStr) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        //时间占位符的处理
        Map<String, String> dayMap = Maps.newHashMap();
        Matcher m = dayPattern.matcher(expressStr);
        while (m.find()) {
            dayMap.put(m.group(0), m.group(1).replace(" ", ""));
        }
        if (dayMap.isEmpty()) {
            return expressStr;
        }
        String newValue = expressStr;
        Date date = null;
        try {
            date = dateFormat.parse(dateStr);
        } catch (ParseException e) {
            throw new JobInterruptedException("date params is invalid!", "date params is invalid!");
        }
        for (Map.Entry<String, String> entry : dayMap.entrySet()) {
            try {
                String eValue = StringUtils.isBlank(entry.getValue()) ? dateFormat.format(date) : getDateNextNDays(date, Integer.valueOf(entry.getValue()));
                newValue = newValue.replace(entry.getKey(), eValue);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return newValue;
    }

    /**
     * 解析字段和条件，便于拼接hdfs目录
     *
     * @param filterExpress
     * @return
     */
    public static Map<String, String> parseConditions(String filterExpress) {
        Map<String, String> expressMap = new HashMap<String, String>();
        if (StringUtils.isBlank(filterExpress)) {
            return expressMap;
        }
        String[] expressArr = filterExpress.split(" AND ");
        for (String ele : expressArr) {
            String[] valueArr = ele.split("=");
            if (valueArr.length != NumberConstant.INT_2) {
                continue;
            }
            String fieldVal = StringUtils.trim(valueArr[1]);
            if (fieldVal.startsWith("\"")) {
                fieldVal = fieldVal.substring(1);
            }
            if (fieldVal.endsWith("\"")) {
                fieldVal = fieldVal.substring(0, fieldVal.length() - 1);
            }
            expressMap.put(StringUtils.trim(valueArr[0]), fieldVal);
        }
        return expressMap;
    }
}

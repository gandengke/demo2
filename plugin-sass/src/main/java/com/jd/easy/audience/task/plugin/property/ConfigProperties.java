package com.jd.easy.audience.task.plugin.property;

import com.jd.easy.audience.common.constant.LabelSourceEnum;
import com.jd.easy.audience.common.constant.StringConstant;
import com.jd.easy.audience.common.exception.JobInterruptedException;
import com.jd.easy.audience.common.oss.OssClientTypeEnum;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Properties;

/**
 * @author cdxiongmei
 * @title ConfigProperties
 * @description 配置文件中属性值
 * @date 2021/8/10 上午10:27
 * @throws
 */
public class ConfigProperties {
    /**
     * 属性对象
     */
    private static Properties properties;
    private static Config templateInfo;

    static {
        try {
            properties = getProperties();
            templateInfo = getTemplate();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 对象存储协议模式
     */
    private static final String JSS_MODE = properties.getProperty("JSS_MODE");
    /**
     * 发布版本
     */
    public static final String VERSION = properties.getProperty("VERSION");
    /**
     * 4a流转统计周期分类-天流转
     */
    public static final String DAY_PERIOD = properties.getProperty("DAY_PERIOD");
    /**
     * 4a流转统计周期分类-周流转
     */
    public static final String WEEK_PERIOD = properties.getProperty("WEEK_PERIOD");
    /**
     * 4a流转统计周期分类-月流转
     */
    public static final String MONTH_PERIOD = properties.getProperty("MONTH_PERIOD");
    /**
     * 4a流转统计周期分类-七天流转
     */
    public static final String RECENT_7DAY_PERIOD = properties.getProperty("RECENT_7DAY_PERIOD");
    /**
     * 4a流转统计周期分类-30流转
     */
    public static final String RECENT_30DAY_PERIOD = properties.getProperty("RECENT_30DAY_PERIOD");
    /**
     * 标签数据集枚举值长度限制
     */
    public static final Integer LABEL_VALUE_LEN_MAX = Integer.valueOf(properties.getProperty("LABEL_VALUE_LEN_MAX"));
    /**
     * 标签数据集枚举值异常字符统一值
     */
    public static final String LABEL_VALUE_DEFAULT = "其他";
    /**
     * 人群生成状态
     */
    public static final String AUDIENCE_STATUS_KEY = "AudienceStatus";
    public static final String ONEID_DETAIL_TB = properties.getProperty("ONEID_DETAIL_TB");
//    private static final String ONE_ID_IDTYPE = "OneID";
    /**
     * 营销回执明细表
     */
    public static final String MARKETING_DETAIL = properties.getProperty("MARKETING_DETAIL");
    //数据库名称
    public static final String JED_DBNAME = properties.getProperty("JED_DBNAME");
    //数据库域名
    public static final String JED_URL = properties.getProperty("JED_URL");
    //数据库用户名
    public static final String JED_USER = properties.getProperty("JED_USER");
    //数据库密码
    public static final String JED_PASSWORD = new String(Base64.getDecoder().decode(properties.getProperty("JED_PASSWORD")), StandardCharsets.UTF_8);

    //ck配置
    //ck数据库名称
    public static final String CK_DBNAME = properties.getProperty("CK_DBNAME");
    //ck数据库域名
    public static final String CK_URL = properties.getProperty("CK_URL");
    //ck数据库用户名
    public static final String CK_USER = properties.getProperty("CK_USER");
    //ck数据库密码
    public static final String CK_PASS = properties.getProperty("CK_PASS");
    //ck数据表后缀
    public static final String CK_TABLESUFFIX = properties.getProperty("CK_TABLESUFFIX");
    //ck数据表前缀
    public static final String CK_TABLEPREFIX = properties.getProperty("CK_TABLEPREFIX");
    public static final String CK_SOCKET_TIMEOUT = properties.getProperty("CK_SOCKET_TIMEOUT");
    //应用端链接
    public static final String SERVICE_URI = properties.getProperty("SERVICE_URI");
    /**
     * 人群包标示类型为oneid
     */
    public static final Long ONEID_ONEIDTYPE = -99L;
    /**
     * oneid自动化应用中无法识别的用户标示类型
     */
    public static final Long ONEID_UNKNOWNTYPE = -1L;

    /**
     * @throws
     * @title getProperties
     * @description 获取属性值
     * @author cdxiongmei
     * @updateTime 2021/8/10 下午7:17
     * @return: java.util.Properties
     */
    private static Properties getProperties() throws IOException {
        Properties properties = new Properties();
        InputStream in = ConfigProperties.class.getResourceAsStream("/config.properties");
        properties.load(new BufferedInputStream(in));
        return properties;
    }

    /**
     * @throws
     * @title getClientType
     * @description 获取oss协议类型（从配置文件读取）
     * @author cdxiongmei
     * @updateTime 2021/8/10 下午7:18
     * @return: com.jd.easy.audience.common.oss.OssClientTypeEnum
     */
    public static OssClientTypeEnum getClientType() {
        if (StringUtils.isBlank(JSS_MODE)) {
            throw new JobInterruptedException("没有配置OSS 协议类型", "Please config your JSSMODE");
        }
        OssClientTypeEnum clientType = OssClientTypeEnum.valueOf(JSS_MODE);
        if (null == clientType) {
            throw new JobInterruptedException("配置OSS 协议类型不合法", "JSSMODE value is invalid");
        }
        return clientType;
    }

    public static String getLabelPrefix(LabelSourceEnum type) {
        return LabelSourceEnum.custom.equals(type) ? StringConstant.CUSTOM_LABELSETS_PREFIX : StringConstant.COMMON_LABELSETS_PREFIX;
    }

    private static Config getTemplate() {
        return ConfigFactory.parseResources("sqlTemplate.conf");
    }

    public static Config getsqlTemplate(String className, String methodName) {
        return ConfigProperties.templateInfo.getConfig(className).getConfig(methodName);
    }

    /**
     * 判断该用户标示是否需要oneid进行关联
     *
     * @param userIdType
     * @return
     */
    public static Boolean isNeedOneIdJoin(Long userIdType) {
        return !(ONEID_ONEIDTYPE.longValue() == userIdType.longValue() || ONEID_UNKNOWNTYPE.longValue() == userIdType.longValue());
    }
}

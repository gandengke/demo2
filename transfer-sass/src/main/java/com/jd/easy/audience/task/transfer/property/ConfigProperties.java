package com.jd.easy.audience.task.transfer.property;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
/**
 * @title ConfigProperties
 * @description 配置文件中属性值
 * @author cdxiongmei
 * @date 2021/8/10 上午10:27
 * @throws
 */
public class ConfigProperties {
    /**
     * 属性对象
     */
     private static Properties properties;

    static {
        try {
            properties = getProperties();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
    * JMQ topic
    */
    public static final String JMQ_TOPIC = properties.getProperty("JMQ_TOPIC");
    /**
     * TOKEN
     */
    /**
     * APP
     */
    public static final String  APP = properties.getProperty("APP");
    /**
     * ADDRESS
     */
    public static final String  ADDRESS = properties.getProperty("ADDRESS");

    public static final String  GROUP = properties.getProperty("GROUP");

    private static final String  WRITER_BATCH_PATH_MODULE = properties.getProperty("WRITER_BATCH_PATH");

    /**
     * @title getProperties
     * @description 获取属性值
     * @author cdxiongmei
     * @updateTime 2021/8/10 下午7:17
     * @return: java.util.Properties
     * @throws
     */
    private static Properties getProperties() throws IOException {
        Properties properties = new Properties();
        InputStream in = ConfigProperties.class.getResourceAsStream("/config.properties");
        properties.load(new BufferedInputStream(in));
        return properties;
    }
    public static String getHdfsPath(String accountName) {
        return String.format(WRITER_BATCH_PATH_MODULE, accountName);
    }
}

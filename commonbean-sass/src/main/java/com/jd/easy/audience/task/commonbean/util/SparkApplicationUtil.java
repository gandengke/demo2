package com.jd.easy.audience.task.commonbean.util;

import com.amazonaws.services.s3.AmazonS3;
import com.jd.easy.audience.common.error.SparkApplicationErrorResult;
import com.jd.easy.audience.common.exception.JobInterruptedException;
import com.jd.easy.audience.common.exception.JobNeedReTryException;
import com.jd.easy.audience.common.model.AppContextJfsConf;
import com.jd.easy.audience.common.model.SparkApplicationContext;
import com.jd.easy.audience.common.oss.OssClientTypeEnum;
import com.jd.easy.audience.common.util.JsonUtil;
import com.jd.easy.audience.common.util.LightOssUtil;
import com.jd.easy.audience.task.commonbean.segment.SparkStepSegment;
import com.jd.jss.JingdongStorageService;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * Created by mzg on 2019/4/25.
 *
 * @author cdxiongmei
 * @version 1.0
 */
public class SparkApplicationUtil {

    /**
     * app argument file name
     */
    public static final String APP_ARGS_FILE_NAME = "args";
    /**
     * app output file name
     */
    public static final String APP_OUTPUT_FILE_NAME = "output";
    /**
     * app error message file name
     */
    public static final String APP_ERROR_FILE_NAME = "error";
    /**
     * app request file name
     */
    public static final String APP_REQUEST_FILE_NAME = "request";
    /**
     * file suffix
     */
    public static final String FILE_SUFFIX = ".json";
    /**
     * 重跑的间隔时间，默认：5分钟
     */
    public static final long NEXT_TRIGGER_TIME = 5 * 60 * 1000L;
    /**
     * 日志类
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkApplicationUtil.class);
    /**
     * upload context to oss(jfs)
     * @param str 待上传文件内容
     * @param filePath osskey
     * @param jfsInfo oss配置
     * @param clientType oss协议类型
     * @return oss文件路径
     * @throws JobNeedReTryException 上传出现异常
     */
    public static String uploadStringToOss(String str, String filePath, AppContextJfsConf jfsInfo, OssClientTypeEnum clientType) throws JobNeedReTryException {
        if (null == jfsInfo || null == clientType) {
            return null;
        }
        if (clientType.getValue().equals(OssClientTypeEnum.S3A.getValue())) {
            AmazonS3 s3aClient = LightOssUtil.createS3aClient(jfsInfo.getEndPoint(), jfsInfo.getAppContextAccessKey(), jfsInfo.getAppContextSecretKey());
            LightOssUtil.writeObjectToOss(s3aClient, str, jfsInfo.getAppContextBucket(), filePath);
        } else {
            JingdongStorageService storageService = null;
            try {
                storageService = LightOssUtil.createJfsClient(jfsInfo.getEndPoint(), jfsInfo.getAppContextAccessKey(), jfsInfo.getAppContextSecretKey());
                LightOssUtil.writeObjectToOss(storageService, str, jfsInfo.getAppContextBucket(), filePath);
            } catch (Exception e) {
                LOGGER.error("uploadStringToOss error: " + e.getMessage(), e);
                throw new JobInterruptedException("uploadStringToOss error!");
            }

        }
        return filePath;
    }

    /**
     * 获取jfs(oss)文件内容
     *
     * @param filePath osskey路径
     * @param jfsInfo oss配置信息
     * @param clientType oss协议类型
     * @return 文件内容
     */
    public static String getFileContext(String filePath, AppContextJfsConf jfsInfo, OssClientTypeEnum clientType) {
        if (null == jfsInfo || null == clientType) {
            return null;
        }
        if (clientType.getValue().equals(OssClientTypeEnum.S3A.getValue())) {
            AmazonS3 s3 = LightOssUtil.createS3aClient(jfsInfo.getEndPoint(), jfsInfo.getAppContextAccessKey(), jfsInfo.getAppContextSecretKey());
            return LightOssUtil.getObject(s3, jfsInfo.getAppContextBucket(), filePath);

        } else {

            JingdongStorageService storageService = null;
            try {
                storageService = LightOssUtil.createJfsClient(jfsInfo.getEndPoint(), jfsInfo.getAppContextAccessKey(), jfsInfo.getAppContextSecretKey());
                return LightOssUtil.getObject(storageService, jfsInfo.getAppContextBucket(), filePath);
            } catch (Exception e) {
                LOGGER.info("get file context error: " + e.getMessage(), e);
                throw new JobInterruptedException("getFileContext error!");
            } finally {
                if (storageService != null) {
                    storageService.destroy();
                }
            }
        }
    }

    /**
     * 获取jfs(oss)文件内容并使用base64解码
     *
     * @param filePath oss key
     * @param jfsInfo oss配置
     * @param clientType oss协议类型
     * @return 解码后oss数据
     */
    public static String downloadByteStringDecoded(String filePath, AppContextJfsConf jfsInfo, OssClientTypeEnum clientType) {

        if (null == jfsInfo || null == clientType || StringUtils.isBlank(filePath)) {
            return null;
        }
        String segment = getFileContext(filePath, jfsInfo, clientType);
        if (segment != null) {
            return new String(Base64.getDecoder().decode(segment), StandardCharsets.UTF_8);
        }
        return segment;
    }

    /**
     * 对传入的对象转成json后进行base64加密，可以屏蔽掉一些特殊字符的干扰，方便传输
     *
     * @param object 需要序列化的原始数据
     * @return 序列化且编码后的数据
     */
    public static String encodeAfterObject2Json(Object object) {
        String serialize = JsonUtil.serialize(object);
        return new String(Base64.getEncoder().encode(serialize.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
    }

    /**
     * 每个作业上下文内容jfs的存放路径
     *
     * @return osskey的格式化后结果
     */
    public static String getFilePath() {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
        String currentDate = sf.format(new Date());
        return String.format("%s/%s", currentDate, UUID.randomUUID().toString());
    }

    /**
     * 获取spark app的错误信息
     *
     * @param filePath 错误输出文件key
     * @param jfsInfo oss配置
     * @param clientType oss协议类型
     * @return spark app的错误类
     */
    public static SparkApplicationErrorResult getSparkApplicationErrorResult(String filePath, AppContextJfsConf jfsInfo, OssClientTypeEnum clientType) {
        String errorFile = filePath + File.separator + APP_ERROR_FILE_NAME + FILE_SUFFIX;
        String decodeJsonContext = downloadByteStringDecoded(errorFile, jfsInfo, clientType);
        if (decodeJsonContext != null) {
            return JsonUtil.deserialize(decodeJsonContext, SparkApplicationErrorResult.class);
        }
        return null;
    }

    /**
     * 获取spark app exception class
     *
     * @param filePath 错误输出文件key
     * @param jfsInfo oss配置
     * @param clientType oss协议类型
     * @return 异常类对象
     */
    public static Exception getExceptionFromSparkErrorResult(String filePath, AppContextJfsConf jfsInfo, OssClientTypeEnum clientType) {
        try {
            SparkApplicationErrorResult errorResult = getSparkApplicationErrorResult(filePath, jfsInfo, clientType);
            if (errorResult != null) {
                return getExceptionOnlyWithMsgFromSparkErrorResult(errorResult);
            }
        } catch (Exception e) {
            return e;
        }
        return null;
    }

    /**
     * construct spark app exception class from `SparkApplicationErrorResult`
     *
     * @param errorResult 异常类信息
     * @return 自定义类对象
     */
    public static Exception getExceptionFromSparkErrorResult(SparkApplicationErrorResult errorResult) {
        String classStr = errorResult.getErrorClass();
        classStr = classStr.replaceFirst("class", "").trim();
        String msg = errorResult.getErrorMsg();
        try {
            Class<? extends Exception> clz = (Class<? extends Exception>) Class.forName(classStr);
            Exception exception = JsonUtil.deserialize(errorResult.getStackTraceJson(), clz);
            return exception;
        } catch (Exception e) {
            LOGGER.error("can not get Exception from SparkApplicationErrorResult, class = {}, msg = {}.", classStr, msg);
        }
        return null;
    }

    /**
     * construct spark app exception class from `SparkApplicationErrorResult`
     * 从错误信息中获取异常信息
     * @param errorResult 自定义异常对象
     * @return 异常类对象
     */
    public static Exception getExceptionOnlyWithMsgFromSparkErrorResult(SparkApplicationErrorResult errorResult) {
        String classStr = errorResult.getErrorClass();
        classStr = classStr.replaceFirst("class", "").trim();
        String msg = errorResult.getErrorMsg();
        try {
            Class<? extends Exception> clz = (Class<? extends Exception>) Class.forName(classStr);
            Constructor c = clz.getConstructor(String.class);
            Exception exception = (Exception) c.newInstance(msg);
            return exception;
        } catch (Exception e) {
            LOGGER.error("can not get Exception from SparkApplicationErrorResult, class = {}, msg = {}.", classStr, msg);
            return new RuntimeException(msg != null ? msg : "null");
        }
    }

    /**
     * build `SparkApplicationContext` object with `SparkSegment`
     *
     * @param segment 业务参数对象
     * @param jfsInfo oss配置
     * @param clientType oss协议类型
     * @return SparkApplicationContext数据对象
     */
    public static SparkApplicationContext getSparkApplicationArgsBean(SparkStepSegment segment, AppContextJfsConf jfsInfo, OssClientTypeEnum clientType) {
        String appContextPath = uploadSegmentArgs(JsonUtil.serialize(segment), jfsInfo, clientType);
        return buildAppArgsBean(appContextPath, jfsInfo);
    }

    /**
     * build `SparkApplicationContext` object with json of `SparkSegment`
     *
     * @param jobContext 数据内容
     * @param jfsInfo oss配置信息
     * @param clientType oss协议类型
     * @return SparkApplicationContext对象
     */
    public static SparkApplicationContext getSparkApplicationArgsBean(String jobContext, AppContextJfsConf jfsInfo, OssClientTypeEnum clientType) {
        String appContextPath = uploadSegmentArgs(jobContext, jfsInfo, clientType);
        return buildAppArgsBean(appContextPath, jfsInfo);
    }

    /**
     * build `SparkApplicationContext` object with appContextPath
     *
     * @param appContextPath context路径信息
     * @param jfsInfo oss对象配置
     * @return 构造SparkApplicationContext对象
     */
    private static SparkApplicationContext buildAppArgsBean(String appContextPath, AppContextJfsConf jfsInfo) {
        SparkApplicationContext args = new SparkApplicationContext();
        args.setAppContextJfsConf(jfsInfo);
        args.setContextPath(appContextPath);
        args.setSparkInputFile(appContextPath + File.separator + APP_ARGS_FILE_NAME + FILE_SUFFIX);
        args.setSparkErrorFile(appContextPath + File.separator + APP_ERROR_FILE_NAME + FILE_SUFFIX);
        args.setSparkOutputFile(appContextPath + File.separator + APP_OUTPUT_FILE_NAME + FILE_SUFFIX);
        return args;
    }

    /**
     * put json of `SparkSegment` to jfs
     *
     * @param jobContext 数据内容
     * @param jfsInfo oss配置
     * @param clientType oss协议类型
     * @return 返回oss对象key
     */
    public static String uploadSegmentArgs(String jobContext, AppContextJfsConf jfsInfo, OssClientTypeEnum clientType) {
        String filePath = jfsInfo.getAppContextBucket() + File.separator + getFilePath();
        String argsFile = filePath + File.separator + APP_ARGS_FILE_NAME + FILE_SUFFIX;
        uploadStringToOss(jobContext, argsFile, jfsInfo, clientType);
        return filePath;
    }

    /**
     * get spark application job result map from oss
     *
     * @param jobContext base64编码的统一任务入口参数数据
     * @param clientType oss协议内省
     * @return map类型任务输出
     */
    public static Map<String, Object> getSparkApplicationResultMap(String jobContext, OssClientTypeEnum clientType) {
        String decodeJsonContext = new String(Base64.getDecoder().decode(jobContext), StandardCharsets.UTF_8);
        LOGGER.info("decode jobContext. keys={}", decodeJsonContext);
        SparkApplicationContext applicationArgs = JsonUtil.deserialize(decodeJsonContext, SparkApplicationContext.class);
        String outputContext = downloadByteStringDecoded(applicationArgs.getSparkOutputFile(),
                applicationArgs.getAppContextJfsConf(), clientType);
        return JsonUtil.deserialize(outputContext, HashMap.class);
    }
}

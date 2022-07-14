package com.jd.easy.audience.task.plugin.run.spark;

import com.jd.easy.audience.common.error.SparkApplicationErrorResult;
import com.jd.easy.audience.common.exception.JobInterruptedException;
import com.jd.easy.audience.common.exception.JobNeedReTryException;
import com.jd.easy.audience.common.model.AppContextJfsConf;
import com.jd.easy.audience.common.model.SparkApplicationContext;
import com.jd.easy.audience.common.util.JdCloudOssUtil;
import com.jd.easy.audience.common.util.JsonUtil;
import com.jd.easy.audience.task.commonbean.segment.SparkStepSegment;
import com.jd.easy.audience.task.commonbean.util.SparkApplicationUtil;
import com.jd.easy.audience.task.driven.run.StepExecutor;
import com.jd.easy.audience.task.driven.segment.Segment;
import com.jd.easy.audience.task.driven.segment.SegmentFactory;
import com.jd.easy.audience.task.plugin.step.exception.StepExceptionEnum;
import com.jd.jss.JingdongStorageService;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

/**
 * @author cdxiongmei
 * @title DatamillSparkPipelineDrivenTest
 * @description pipeline spark 作业入口
 * @date 2021/8/10 上午10:34
 * @throws
 */
public class DatamillSparkPipelineDriven {
    /**
     * log工具
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(DatamillSparkPipelineDriven.class);
    /**
     * @title main
     * @description 任务统一调用入口
     * @author cdxiongmei
     * @param: args
     * @updateTime 2021/8/10 下午5:22
     * @throws
     */
    public static void main(String[] args) throws Exception {
        if (args.length < BigInteger.ONE.intValue()) {
            throw new RuntimeException("Missing pipeline argument.");
        }
        //1.参数base64解码+反序列化
        String segmentJson = args[BigInteger.ZERO.intValue()];
        String decodeJsonContext = new String(Base64.getDecoder().decode(segmentJson), StandardCharsets.UTF_8);
        LOGGER.info("DatamillSparkPipelineDriven arg: {}", decodeJsonContext);
        SparkApplicationContext applicationArgs = JsonUtil.deserialize(decodeJsonContext, SparkApplicationContext.class);

        //2.获取step参数
        AppContextJfsConf jfsInfo = applicationArgs.getAppContextJfsConf();
        SparkStepSegment sparkSegment;
        JingdongStorageService jfsClient = JdCloudOssUtil.createJfsClient(jfsInfo.getEndPoint(), jfsInfo.getAppContextAccessKey(), jfsInfo.getAppContextSecretKey());
        try {
            //2.1 获取oss中step的段落数据
            String segment = JdCloudOssUtil.getObject(jfsClient, jfsInfo.getAppContextBucket(), applicationArgs.getSparkInputFile());
            LOGGER.info("segment:{}", segment);
            Segment seg = SegmentFactory.create(segment);
            if (seg instanceof SparkStepSegment) {
                sparkSegment = (SparkStepSegment) seg;
            } else {
                LOGGER.error("segment is wrong segment={}", seg.toString());
                throw new RuntimeException("segment is wrong");
            }
        } catch (Exception e) {
            LOGGER.error("DatamillSparkPipelineDriven deserialize error: ", e);
            throw new RuntimeException("Wrong pipeline segment define", e);
        }
        try {
            //3.清理之前的异常文件和结果输出文件
            if (JdCloudOssUtil.objectExist(jfsClient, jfsInfo.getAppContextBucket(), applicationArgs.getSparkErrorFile())) {
                jfsClient.deleteObject(jfsInfo.getAppContextBucket(), applicationArgs.getSparkErrorFile());
            }
            if (JdCloudOssUtil.objectExist(jfsClient, jfsInfo.getAppContextBucket(), applicationArgs.getSparkOutputFile())) {
                jfsClient.deleteObject(jfsInfo.getAppContextBucket(), applicationArgs.getSparkOutputFile());
            }
            //4.任务执行
            Map<String, Object> outputMap = StepExecutor.run(sparkSegment.getName(), sparkSegment.getBeans());
            LOGGER.info("spark application finished.");
            if (outputMap != null && outputMap.size() > BigInteger.ZERO.intValue()) {
                //4.1 写结果输出到oss中的output文件中
                LOGGER.info("map {} will be write to oss path {}.", outputMap, applicationArgs.getSparkOutputFile());
                String objectStr = SparkApplicationUtil.encodeAfterObject2Json(outputMap);
                JdCloudOssUtil.writeObjectToOss(jfsClient, objectStr, jfsInfo.getAppContextBucket(), applicationArgs.getSparkOutputFile());
            }
        } catch (Exception e) {
            //5.异常统一处理
            if (e.getCause() == null) {
                //5.1 系统错误
                buildAppError(jfsClient, new JobNeedReTryException("系统错误", "Error: System Error!"), applicationArgs);
                throw new RuntimeException("Wrong pipeline running", e);
            }
            String className = e.getCause().getClass().getSimpleName();
            if (e.getCause() instanceof JobInterruptedException) {
                //5.2 已经处理的异常-无需重试
                buildAppError(jfsClient, (JobInterruptedException) e.getCause(), applicationArgs);
            } else if (e.getCause() instanceof JobNeedReTryException) {
                //5.3 已经处理的异常-需重试
                buildAppError(jfsClient, (JobNeedReTryException) e.getCause(), applicationArgs);
            } else {
                //5.4 未处理异常，需要输出具体的异常内容
                SparkApplicationErrorResult errorResult = new SparkApplicationErrorResult();
                StackTraceElement stackTraceElement = e.getCause().getStackTrace()[BigInteger.ZERO.intValue()];
                LOGGER.error("xmcauseby: ", e.getCause());
                LOGGER.error("异常名：" + stackTraceElement.toString());
                LOGGER.error("异常类名：" + stackTraceElement.getFileName());
                LOGGER.error("异常方法名：" + stackTraceElement.getMethodName());
                String stepName = stackTraceElement.getFileName().split("\\.")[BigInteger.ZERO.intValue()];
                String methodName = stackTraceElement.getMethodName();
                StepExceptionEnum exceptionEnum = StepExceptionEnum.valueOf(stepName, methodName);
                Exception exception = null;
                if (null != exceptionEnum && exceptionEnum.isNeedRetry()) {
                    exception = new JobNeedReTryException(exceptionEnum.getExceptionDesc(), exceptionEnum.getExceptionEnDesc(), e);
                } else if (null != exceptionEnum && !exceptionEnum.isNeedRetry()) {
                    exception = new JobInterruptedException(exceptionEnum.getExceptionDesc(), exceptionEnum.getExceptionEnDesc(), e);
                } else {
                    exception = new JobNeedReTryException("系统错误", "Error: System Error!", e.getCause());
                }
                buildAppError(jfsClient, exception, applicationArgs);
            }
            if (!(e.getCause() instanceof JobInterruptedException)) {
                //5.3 已经处理的异常-需重试
                throw new RuntimeException("Wrong pipeline running", e);
            }
        } finally {
//            jfsClient.destroy();
            StepExecutor.shutdownThreadPool();
            SparkSession.builder().getOrCreate().stop();
        }
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
    private static void buildAppError(JingdongStorageService jfsClient, Exception e, SparkApplicationContext applicationArgs) {
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
//        LOGGER.error("errorResult string={}", serialize(errorResult));
        String objectStr = SparkApplicationUtil.encodeAfterObject2Json(errorResult);
        JdCloudOssUtil.writeObjectToOss(jfsClient, objectStr, applicationArgs.getAppContextJfsConf().getAppContextBucket(), applicationArgs.getSparkErrorFile());
    }

}

package com.jd.easy.audience.task.plugin.util;

import com.jd.easy.audience.common.constant.NumberConstant;
import com.jd.easy.audience.common.model.param.DatasetUserIdConf;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Spark UDF util
 * @author yuxiaoqing
 * @date 2019-09-29
 */
public class SparkUdfUtil {

    /**
     * 判断4a状态明细压缩包中，某个时间段是否包含某个4a状态
     */
    public static boolean containsStatus(WrappedArray<Integer> statusSeq, int fromIndex, int toIndex, WrappedArray<Integer> status) {
        List<Integer> statusSeqList = (List<Integer>) JavaConversions.seqAsJavaList(statusSeq);
        List<Integer> statusList = (List<Integer>) JavaConversions.seqAsJavaList(status);
        boolean contains = false;
        for (Integer subStatus : statusList) {
            contains = statusSeqList.subList(fromIndex, toIndex).contains(subStatus);
            if (contains) {
                break;
            }
        }
        return contains;
    }

    /**
     * 字符串是否全部为数字
     */
    public static boolean isDigit(String str) {
        for (int i = str.length(); --i >= 0; ) {
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 将csv重命名，只修改文件名
     * @param conf
     * @param filePath
     * @param newFileName
     */
    @Deprecated
    public static void renameHdfsFile(Configuration conf, String filePath, String newFileName) {
        try {
            FileSystem fileSystem = FileSystem.get(conf);
            Path tempPath = new Path(filePath);
            Path newPath = new Path(filePath + "/" + newFileName);
            Path successPath = new Path(filePath + "/_SUCCESS");
            if (fileSystem.exists(successPath)) {
                fileSystem.delete(successPath, true);
            }
            Path csvPath = fileSystem.listStatus(tempPath)[0].getPath();
            System.out.println("modified source file =" + csvPath.getName() + " to dest file =" + tempPath.getName() + newFileName);
            fileSystem.rename(csvPath, newPath);
//            fileSystem.delete(tempPath, true);
        } catch (IOException e) {
           System.out.println("copy file to jd cloud oss error:" + e.getMessage());
        }
    }
    /**
     * @title dealPath
     * @description 处理oss路径前面和后面的斜杠
     * @author cdxiongmei
     * @param: sourcePath
     * @updateTime 2021/8/10 下午8:16
     * @return: java.lang.String
     * @throws
     */
    public static String dealPath(String sourcePath) {
        StringBuilder filePath = new StringBuilder(sourcePath);
        if (sourcePath.endsWith(File.separator)) {
            filePath.deleteCharAt(filePath.length() - BigInteger.ONE.intValue());
        }
        if (sourcePath.startsWith(File.separator)) {
            filePath.deleteCharAt(BigInteger.ZERO.intValue());
        }
        return filePath.toString();
    }
    public static List<String> getUserIdsFromOneId(List<DatasetUserIdConf> tableOneIdConf) {
        List<String> userIds = new ArrayList<>();
        Map<Long, List<DatasetUserIdConf>> sameIdTypeMap = tableOneIdConf.stream().collect(Collectors.groupingBy(DatasetUserIdConf::getUserIdType));
        sameIdTypeMap.forEach((idType, sameTypeConfig) -> {
            String sourceField = StringUtils.join(sameTypeConfig.stream().map(curConfig -> "CAST(" + curConfig.getUserField() + " AS string)").collect(Collectors.toList()), ",");
            if (sameTypeConfig.size() > NumberConstant.INT_1) {
                userIds.add("COALESCE(" + sourceField + ") AS user_" + idType);
            } else {
                userIds.add(sourceField + " AS user_" + idType);
            }
        });
        return userIds;
    }
}

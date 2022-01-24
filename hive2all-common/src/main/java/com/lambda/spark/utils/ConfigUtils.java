package com.lambda.spark.utils;

import org.apache.spark.SparkConf;

import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * @Author: zhangxinsen
 * @Date: 2022/1/24 2:02 PM
 * @Desc:
 * @Version: v1.0
 */

public class ConfigUtils {
    public static void configSparkConf(SparkConf sparkConf, Map<String, String> configuration) {
        for (Map.Entry<String, String> stringStringEntry : configuration.entrySet()) {
            sparkConf.set(stringStringEntry.getKey(), stringStringEntry.getValue());
        }
    }

    public static List<String> getTableLists(Map<String, String> configuration) {
        List<String> tableNameList = new LinkedList<>();
        for (Map.Entry<String, String> stringStringEntry : configuration.entrySet()) {
            if (stringStringEntry.getKey().startsWith("table-")) {
                tableNameList.add(stringStringEntry.getValue());
            }
        }
        return tableNameList;
    }

    public static String getOutputDir(Map<String, String> configuration) {
        String outputDir = null;
        String s = configuration.get("output.dir");
        if (s == null) {
            outputDir = "output";
        } else {
            outputDir = s;
        }
        return outputDir;
    }

    public static boolean getIsLimitEnable(Map<String, String> configuration) {
        return configuration.get("limit.enable") != null && configuration.get("limit.enable").equals("true");
    }

    public static boolean getIsKeywordEnable(Map<String, String> configuration, String keyword) {
        return configuration.get(keyword) != null && configuration.get(keyword).equals("true");
    }

    public static int getLimitNum(Map<String, String> configuration) {
        int limitNum = 100;
        if (configuration.get("limit.num") == null) {
            limitNum = 100;
        } else {
            limitNum = Integer.parseInt(configuration.get("limit.num"));
        }
        return limitNum;
    }

    public static boolean getIsCompressionEnable(Map<String, String> configuration) {
        String compressionFormat = configuration.get("compression.format");
        if (compressionFormat == null) {
            return false;
        }
        return !compressionFormat.toLowerCase(Locale.ROOT).equals("none");
    }

    public static String getCompressionFormat(Map<String, String> configuration) {
        String compressionFormat = configuration.get("compression.format");
        return compressionFormat;
    }
}

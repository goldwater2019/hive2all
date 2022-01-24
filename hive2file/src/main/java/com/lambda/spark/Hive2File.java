package com.lambda.spark;


// $example on:spark_hive$

import com.lambda.spark.utils.FileUtils;
import com.lambda.spark.utils.SparkSQLUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;

import static com.lambda.spark.utils.ConfigUtils.*;
import static com.lambda.spark.utils.XMLUtils.getConfigurationInDir;
import static com.lambda.spark.utils.XMLUtils.getFilterConditionsInDir;


/**
 * @Author: zhangxinsen
 * @Date: 2022/1/22 1:02 PM
 * @Desc:
 * @Version: v1.0
 */

public class Hive2File {

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf();
        String configDir = null;
        for (String arg : args) {
            if (arg.startsWith("--config-dir=")) {
                String[] split = arg.split("=");
                if (split.length != 2) {
                    continue;
                }
                String dirname = split[1];
                configDir = dirname.trim();
            }
        }
        if (configDir == null) {
            configDir = "src/main/resources";
        }
        System.out.println("conf path: " + configDir);
        Map<String, String> configuration = getConfigurationInDir(configDir);
        List<String> tableNameList = getTableLists(configuration);
        configSparkConf(sparkConf, configuration);
        String outputDir = getOutputDir(configuration);  // 输出路径
        boolean isLimitEnable = getIsKeywordEnable(configuration, "limit.enable");  // 是否限制
        int limitNum = getLimitNum(configuration);  // 限制大小
        boolean isShowEnable = getIsKeywordEnable(configuration, "show.enable");  // 是否show
        boolean isWriteEnable = getIsKeywordEnable(configuration, "write.enable");  // 是否写文件
        boolean isCompressionEnable = getIsCompressionEnable(configuration); // 是否需要压缩
        String compressionFormat = getCompressionFormat(configuration);  // 压缩格式

        System.out.println(MessageFormat.format("output dir: {0}, isLimitEnable:{1}, limitSize:{2}, isShowEnable:{3}" +
                ", isWriteEnable{4}", outputDir, isLimitEnable, limitNum, isShowEnable, isWriteEnable));
        FileUtils.createDir(outputDir);

        // $example on:spark_hive$
        // warehouseLocation points to the default location for managed databases and tables
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]") // set master
                .appName("Java Spark Hive Example")
                .config(sparkConf)
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();


        for (String tableName : tableNameList) {
            FileUtils.clearDir(MessageFormat.format("{0}/{1}", outputDir, tableName));
            Dataset<Row> ddl = SparkSQLUtils.getTableCreateDll(spark, tableName);
            JavaRDD<String> stringJavaRDD = SparkSQLUtils.getAllFieldsFromDllDataSet(ddl);
            List<String> fields = stringJavaRDD.collect();
            String fieldsJoinString = String.join(",", fields);

            String selectSql = MessageFormat.format("select {0} from {1}", fieldsJoinString, tableName);
            // 加上筛选条件
            Map<String, List<String>> filterConditions = getFilterConditionsInDir(configDir, configuration, tableName);
            for (Map.Entry<String, List<String>> stringListEntry : filterConditions.entrySet()) {
                String clauseKey = stringListEntry.getKey();
                List<String> conditionList = stringListEntry.getValue();
                String clauseValue = String.join(" and ", conditionList);
                selectSql = MessageFormat.format("{0} {1} {2}", selectSql, clauseKey, clauseValue);
            }
            if (isLimitEnable) {
                selectSql = MessageFormat.format("select {0} from {1} limit {2}", fieldsJoinString, tableName, limitNum);
            }
            selectSql = selectSql + " ;";
            System.out.println("select sql: " + selectSql);
            Dataset<Row> sql = spark.sql(selectSql);
            if (isShowEnable) {
                ddl.show();
                sql.show();
            }
            if (isWriteEnable) {
                ddl.write().text(MessageFormat.format("{0}/{1}/ddl", outputDir, tableName));
                if (isCompressionEnable) {
                    sql.write()
                            .option("compression", compressionFormat)  // gzip, snappy, lzo and so on
                            .json(MessageFormat.format("{0}/{1}/json", outputDir, tableName));
                } else {
                    sql.write()
                            .json(MessageFormat.format("{0}/{1}/json", outputDir, tableName));
                }

            }
        }
        spark.stop();
    }
}

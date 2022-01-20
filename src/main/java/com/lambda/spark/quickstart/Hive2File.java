package com.lambda.spark.quickstart;

/**
 * @Author: zhangxinsen
 * @Date: 2022/1/20 5:32 PM
 * @Desc:
 * @Version: v1.0
 */

// $example on:spark_hive$

import com.lambda.spark.quickstart.utils.FileUtils;
import com.lambda.spark.quickstart.utils.SparkSQLUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.text.MessageFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.lambda.spark.quickstart.utils.XMLUtils.getConfigurationInDir;
// $example off:spark_hive$

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
        List<String> tableNameList = new LinkedList<>();
        Map<String, String> configuration = getConfigurationInDir(configDir);
        for (Map.Entry<String, String> stringStringEntry : configuration.entrySet()) {
            sparkConf.set(stringStringEntry.getKey(), stringStringEntry.getValue());
            if (stringStringEntry.getKey().startsWith("table-")) {
                tableNameList.add(stringStringEntry.getValue());
            }
        }

        String outputDir = null;
        String s = configuration.get("output.dir");
        if (s == null) {
            outputDir = "output";
        } else {
            outputDir = s;
        }

        boolean isLimitEnable = false;
        if (configuration.get("limit.enable") == null || !configuration.get("limit.enable").equals("true")) {
            isLimitEnable = false;
        } else {
            isLimitEnable = true;
        }

        int limitNum = 100;
        if (configuration.get("limit.num") == null) {
            limitNum = 100;
        } else {
            limitNum = Integer.valueOf(configuration.get("limit.num"));
        }

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
            if (isLimitEnable) {
                selectSql = MessageFormat.format("select {0} from {1} limit {2}", fieldsJoinString, tableName, limitNum);
            }
            Dataset<Row> sql = spark.sql(selectSql);
            ddl.write().text(MessageFormat.format("{0}/{1}/ddl", outputDir, tableName));
            sql.write()
                    .option("compression", "gzip")
                    .json(MessageFormat.format("{0}/{1}/json", outputDir, tableName));
        }
        spark.stop();
    }
}
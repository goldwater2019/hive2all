package com.lambda.spark.quickstart;

/**
 * @Author: zhangxinsen
 * @Date: 2022/1/20 5:32 PM
 * @Desc:
 * @Version: v1.0
 */

// $example on:spark_hive$

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.lambda.spark.quickstart.utils.XMLUtils.getConfigurationInDir;
// $example off:spark_hive$

public class WordCount {

    // $example on:spark_hive$
    public static class Record implements Serializable {
        private int key;
        private String value;

        public int getKey() {
            return key;
        }

        public void setKey(int key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
    // $example off:spark_hive$

    public static void main(String[] args) {
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
        Map<String, String> configuration = getConfigurationInDir(configDir);
        for (Map.Entry<String, String> stringStringEntry : configuration.entrySet()) {
            sparkConf.set(stringStringEntry.getKey(), stringStringEntry.getValue());
        }

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

        Dataset<Row> sql = spark.sql("select distinct dt from edw_cdm.dwd_fact_cs_fixduty;");
        sql.show();

        spark.stop();
    }
}
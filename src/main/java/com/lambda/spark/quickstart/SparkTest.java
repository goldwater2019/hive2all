package com.lambda.spark.quickstart;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @Author: zhangxinsen
 * @Date: 2022/1/21 12:44 AM
 * @Desc:
 * @Version: v1.0
 */

public class SparkTest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]") // set master
                .appName("Java Spark Hive Example")
                // .config(sparkConf)
                // .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();

        Dataset<Row> json = spark.read().option("compression", "gzip").json("/Users/zhangxinsen/workspace/hive2all/output2/edw_cdm.dwd_fact_cs_fixduty/json/part-00000-a11e1fa0-5910-4183-8f71-982887d616e0-c000.json.gz");
        json.show();
        spark.stop();
    }
}

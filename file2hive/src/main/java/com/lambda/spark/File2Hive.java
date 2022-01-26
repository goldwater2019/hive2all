package com.lambda.spark;

import com.lambda.spark.utils.SparkSQLUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructField;
import scala.Tuple2;
import scala.collection.Iterator;

import java.io.File;
import java.text.MessageFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.lambda.spark.utils.ConfigUtils.getOutputDir;
import static com.lambda.spark.utils.XMLUtils.getConfigurationInDir;


/**
 * @Author: zhangxinsen
 * @Date: 2022/1/22 1:05 PM
 * @Desc:
 * @Version: v1.0
 */

public class File2Hive {
    public static void describeTable(SparkSession spark, String dataDirname, String tableName) {
        Dataset<Row> json = spark.read().option("compression", "gzip").json(dataDirname);
        json.show();
        json.createOrReplaceTempView(tableName);
        System.out.println("========== printSchema ==========");
        json.printSchema();
        System.out.println("========== count ==========");
        Dataset<Row> count = spark.sql(MessageFormat.format("SELECT count(1) FROM {0} WHERE 1=1;", tableName));
        count.show();
    }

    public static void loadAndInsertIntoHive(SparkSession spark, String taskTableName, Map<String, String> configuration) {

        String outputDir = getOutputDir(configuration);

        String ddlPath = MessageFormat.format("file://{0}/{1}/ddl", outputDir, taskTableName);
        String dataPath = MessageFormat.format("file://{0}/{1}/json", outputDir, taskTableName);

        Dataset<Row> text = spark.read().text(ddlPath);
        Dataset<String> stringDataset = text.map(new MapFunction<Row, String>() {
                                                     @Override
                                                     public String call(Row value) throws Exception {
                                                         return value.getString(0);
                                                     }
                                                 },
                Encoders.STRING()
        );
        Dataset<String> ddlHeaderDS = stringDataset.filter(new FilterFunction<String>() {
            @Override
            public boolean call(String value) throws Exception {
                String s1 = value.toLowerCase(Locale.ROOT);
                if (s1.contains("create table")) {
                    return true;
                }
                return false;
            }
        });
        Dataset<Tuple2<String, String>> dbNameAndTableName = ddlHeaderDS.map(new MapFunction<String, Tuple2<String, String>>() {
                                                                                 @Override
                                                                                 public Tuple2<String, String> call(String value) throws Exception {
                                                                                     String replace = value.replace("`", "");
                                                                                     String[] split = replace.trim().split("\\s+");
                                                                                     String dbAndTable = split[2];
                                                                                     String[] split1 = dbAndTable.split("\\.");
                                                                                     return new Tuple2<>(split1[0], split1[1]);
                                                                                 }
                                                                             }, Encoders.tuple(Encoders.STRING(), Encoders.STRING())
        );

        Tuple2<String, String> stringStringTuple2 = ((Tuple2<String, String>[]) dbNameAndTableName.collect())[0];
        String dbName = stringStringTuple2._1();
        String tableName = stringStringTuple2._2();

        String oldDBName = dbName;
        String newDBName = dbName;
        if (configuration.get("dbname.replace.old") != null) {
            oldDBName = configuration.get("dbname.replace.old");
        }

        if (configuration.get("dbname.replace.new") != null) {
            newDBName = configuration.get("dbname.replace.new");
        }

        List<String> replaceKeyList = new LinkedList<>();
        for (String s : configuration.keySet()) {
            if (s.contains("replace")) {
                String splitString = null;
                if (s.endsWith(".new")) {
                    splitString = "\\.new";
                } else {
                    splitString = "\\.old";
                }
                String replaceKey = s.split(splitString)[0];
                replaceKeyList.add(replaceKey);
            }
        }

        // 获得最新的ddl
        Dataset<String> ddlDS = stringDataset.map(new MapFunction<String, String>() {
                    @Override
                    public String call(String value) throws Exception {
                        if (value.contains("CREATE TABLE")) {
                            return value.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
                        }
                        return value;
                    }
                }, Encoders.STRING())
                .map(new MapFunction<String, String>() {
                    @Override
                    public String call(String value) throws Exception {
                        for (String replaceKey : replaceKeyList) {
                            String newKey = MessageFormat.format("{0}.new", replaceKey);
                            String oldKey = MessageFormat.format("{0}.old", replaceKey);
                            String newStringValue = configuration.get(newKey);
                            String oldStringValue = configuration.get(oldKey);
                            if (newStringValue == null || oldStringValue == null) {
                                continue;
                            }
                            if (value.contains(oldStringValue)) {
                                value = value.replace(oldStringValue, newStringValue);
                            }
                        }
                        return value;
                    }
                }, Encoders.STRING());

        // 获得partition的信息
        Dataset<String> partitionedByDS = ddlDS.filter(new FilterFunction<String>() {
            @Override
            public boolean call(String value) throws Exception {
                // PARTITIONED BY (dt)
                if (value.contains("PARTITIONED BY ")) {
                    return true;
                }
                return false;
            }
        }).map(new MapFunction<String, String>() {
            @Override
            public String call(String value) throws Exception {
                String s1 = value.split(" BY \\(")[1];  // dt,at)
                String s2 = s1.split("\\)")[0];  // dt, at, uv
                String[] split = s2.split(",");
                String[] temp = new String[split.length];
                for (int i = 0; i < split.length; i++) {
                    temp[i] = split[i].trim();
                }
                return String.join(",", temp);
            }
        }, Encoders.STRING());
        long count = partitionedByDS.count();
        boolean isPartitionEnable = count != 0;

        String partitionStrList = null;
        if (isPartitionEnable) {
            partitionStrList = partitionedByDS.first();
        }
        System.out.println(partitionStrList);


        String ddlString = String.join("\n", (String[]) (ddlDS.collect()));

        // System.out.println(ddlString);
        spark.sql(ddlString).show();  // 建表

        String ddlTableName = ddlString.replace("`", "").split("IF NOT EXISTS ")[1].split("\\s+")[0];
        Dataset<Row> ddl = spark.sql(MessageFormat.format("show create table {0};", ddlTableName));
        JavaRDD<String> allFieldsFromDllDataSet = SparkSQLUtils.getAllFieldsFromDllDataSet(ddl);
        List<String> fieldNameList = allFieldsFromDllDataSet.collect();


        dbName = dbName.replace(oldDBName, newDBName);

        // load 数据
        // dataPath = "/Users/zhangxinsen/workspace/hive2all/output/edw_cdm.dwd_fact_lb_hs_ewbs_list_no/json/part-00003-70b1a2f4-99e2-4505-b67d-a672796bbada-c000.json.gz";
        Dataset<Row> json = spark.read().option("compression", "gzip").json(dataPath);
        json.createOrReplaceTempView(tableName);
        // dt 相关
        boolean isJsonHasDtField = false;
        Iterator<StructField> structFieldIterator = json.schema().toIterator();
        while (structFieldIterator.hasNext()) {
            StructField structField = structFieldIterator.next();
            String name = structField.name();
            if (name.equals("dt")) {
                isJsonHasDtField = true;
                break;
            }
        }
        boolean isDllHashMonthField = ddlString.contains("month");
        if (isJsonHasDtField && isDllHashMonthField) {
            List<String> temp = new LinkedList<>();
            for (String fieldName : fieldNameList) {
                if (fieldName.equals("month")) {
                    continue;
                }
                temp.add(fieldName);
            }
            String selectSQL = MessageFormat.format(
                    "select {0}, substr(dt, 0, 6) month from {1}",
                    String.join(",", temp),
                    tableName
            );
            System.out.println(selectSQL);
            json = spark.sql(
                    selectSQL
            );
        }

        json.show();
        if (isPartitionEnable) {
            String[] partitionColumns = partitionStrList.split(",");
            json.write().mode(SaveMode.Overwrite).format("hive").partitionBy(partitionColumns)
                    .saveAsTable(MessageFormat.format("`{0}`.`{1}`",
                            dbName, tableName));
        } else {
            json.write().mode(SaveMode.Overwrite).format("hive")
                    .saveAsTable(MessageFormat.format("`{0}`.`{1}`",
                            dbName, tableName));
        }
    }

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


        // $example on:spark_hive$
        // warehouseLocation points to the default location for managed databases and tables
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]") // set master
                .appName("Java Spark Load JSON.GZ INTO HIVE")
                .config(sparkConf)
                .config("spark.executor.instances", 8)
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();
//        describeTable(spark, "output/edw_cdm.dwd_fact_cas_login_log_delta_1d/json", "dwd_fact_cas_login_log_delta_1d");
//        describeTable(spark, "output/edw_cdm.dwd_fact_cs_fixduty/json", "dwd_fact_cs_fixduty");
//        describeTable(spark, "output/edw_cdm.dwd_fact_cs_mistake/json", "dwd_fact_cs_mistake");

        System.out.println("切换库");
        spark.sql("use edw_cdm;");
        // System.out.println("删除表");
        // spark.sql("select count(1) from edw_cdm.dws_operation_load_rate_by_road").show();
        // spark.sql("drop table if exists edw_cdm.dws_operation_load_rate_by_road").show();
        System.out.println("查看表");
        spark.sql("show tables;").show();

        for (String taskTableName : tableNameList) {
            loadAndInsertIntoHive(spark, taskTableName, configuration);
        }
        spark.stop();
    }
}

package com.lambda.spark;

import com.lambda.spark.utils.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.io.File;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;

import static com.lambda.spark.utils.ConfigUtils.*;
import static com.lambda.spark.utils.XMLUtils.getConfigurationInDir;
import static com.lambda.spark.utils.XMLUtils.getSqlConfigInDir;

/**
 * @Author: zhangxinsen
 * @Date: 2022/1/24 10:18 PM
 * @Desc:
 * @Version: v1.0
 */

public class SSqlClient {
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

        // 注册UDF
        spark.udf().register("income_gambling_segmented_split",
                new IncomeGamblingToSegmentedSplit(),
                DataTypes.createArrayType(DataTypes.StringType));

        // 获得SQL内容, 并且输出
        List<Map<String, String>> sqlConfigInDir = getSqlConfigInDir(configDir);
        for (Map<String, String> sqlConfig : sqlConfigInDir) {
            String sqlName = sqlConfig.get("name");
            String sqlValue = sqlConfig.get("value");
            boolean isSqlEnable = sqlConfig.get("isSqlEnable").equals("true");
            String description = sqlConfig.get("description");
            if (isSqlEnable) {
                System.out.println("description: " + description);
                Dataset<Row> dataset = spark.sql(sqlValue);
                dataset.createOrReplaceTempView(sqlName);
                // dataset.show();
            }
        }
        System.out.println("所有视图创建完毕");
        spark.sql("use edw_cdm;");
        spark.sql("show tables;").show();

//        Dataset<Row> sql = spark.sql("select * from dws_operation_load_rate_by_road_result");
//        sql.show();
//        sql.write().json(MessageFormat.format("file://{0}/dws_operation_load_rate_by_road_result", outputDir));
        String resultSQL = "select\n" +
                "20220123 as dt,\n" +
                "   stat_date,\n" +
                "   shipment_gid,\n" +
                "   order_id,\n" +
                "   shipment_schedule_id,\n" +
                "   car_id,\n" +
                "   actual_cost,\n" +
                "   order_type,\n" +
                "   line_type,\n" +
                "   please_car_nature,\n" +
                "   load_mode,\n" +
                "   car_type,\n" +
                "   rated_capacity_total,\n" +
                "   rated_vol_total,\n" +
                "   transport_status,\n" +
                "   overtime_reason,\n" +
                "   overtime_son_reason,\n" +
                "   plan_start_time,\n" +
                "   planned_schedule,\n" +
                "   actual_schedule,\n" +
                "   trans_rodes,\n" +
                "   by_road_method,\n" +
                "   road_rated_capacity,\n" +
                "   road_rated_vol,\n" +
                "   road_planned_schedule,\n" +
                "   road_actual_schedule,\n" +
                "   first_rodes,\n" +
                "   first_distribute_nm,\n" +
                "   first_prov_nm,\n" +
                "   next_rodes,\n" +
                "   next_distribute_nm,\n" +
                "   next_prov_nm,\n" +
                "   road_out_piece_num,\n" +
                "   ewb_num road_ewb_num,\n" +
                "   road_calc_weight,\n" +
                "   road_operate_weight,\n" +
                "   road_vol,\n" +
                "   mini_big_clac_weight,\n" +
                "   mini_small_clac_weight,\n" +
                "   dsd_calc_weight,\n" +
                "   jzld_piece_num,\n" +
                "   jzld_calc_weight,\n" +
                "   phd_piece_num,\n" +
                "   phd_calc_weight,\n" +
                "   load_rate\n" +
                "from\n" +
                "   result_total;";
        Dataset<Row> result = spark.sql(resultSQL);
        result.write()
                .option("fileFormat", "orc")
                .mode(SaveMode.Append)
                .format("hive")
                .partitionBy("dt")
                .saveAsTable("`edw_cdm`.`dws_operation_load_rate_by_road`");
        spark.stop();
    }
}

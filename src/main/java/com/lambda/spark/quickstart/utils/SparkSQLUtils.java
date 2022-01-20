package com.lambda.spark.quickstart.utils;

import groovy.sql.DataSet;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @Author: zhangxinsen
 * @Date: 2022/1/21 12:08 AM
 * @Desc:
 * @Version: v1.0
 */

public class SparkSQLUtils {
    public static JavaRDD<String> getAllFieldsFromDllDataSet(Dataset<Row> ddl) {
        JavaRDD<String> stringJavaRDD = ddl.javaRDD().flatMap(new FlatMapFunction<Row, String>() {
                    @Override
                    public Iterator<String> call(Row row) throws Exception {
                        String ddl = row.getString(0);
                        String[] split = ddl.split("` \\(");
                        if (split.length < 2) {
                            return null;
                        }
                        String temp = "";
                        for (int i = 1; i < split.length; i++) {
                            temp = temp + split[i];
                        }
                        String[] split1 = temp.split("\\)\n");
                        String fieldsString = split1[0];
                        return Arrays.stream(fieldsString.split("\\n")).iterator();
                    }
                })
                .filter(new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String v1) throws Exception {
                        if (v1.trim().length() > 0 && v1.contains("`")) {
                            return true;
                        }
                        return false;
                    }
                })
                .map(new Function<String, String>() {
                    @Override
                    public String call(String line) throws Exception {
                        line = line.trim();
                        line = line.replace("`", "");
                        line = line.split("\\s+")[0];
                        return line;
                    }
                });
        return stringJavaRDD;
    }

    public static Dataset<Row> getTableCreateDll(SparkSession spark, String tableName) {
        String sql = MessageFormat.format("show create table {0};", tableName);
        Dataset<Row> rowDataset = spark.sql(sql);
        return rowDataset;
    }
}

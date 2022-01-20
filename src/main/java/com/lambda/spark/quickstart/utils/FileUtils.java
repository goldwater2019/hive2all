package com.lambda.spark.quickstart.utils;

import java.io.File;

/**
 * @Author: zhangxinsen
 * @Date: 2022/1/21 12:25 AM
 * @Desc:
 * @Version: v1.0
 */

public class FileUtils {
    public static void clearDir(String dirname) throws Exception {
        File file = new File(dirname);
        if (!file.exists()) {
            createDir(dirname);
        }
        if (!file.isDirectory()) {
            throw new Exception("only support dir");
        }
        File[] files = file.listFiles();
        for (File file1 : files) {
            clearFile(file1);
        }
    }
    public static void clearFile(File file) {
        if (file.isFile()) {
            file.delete();
            return;
        }
        for (File listFile : file.listFiles()) {
            clearFile(listFile);
        }
        file.delete();
    }

    public static void createDir(String dirname) throws Exception {
        File file = new File(dirname);
        if (file.isFile()) {
            throw new Exception("only support dir");
        }
        if (file.exists()) {
            return;
        }
        file.mkdir();
    }
}

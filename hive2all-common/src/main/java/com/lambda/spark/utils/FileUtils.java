package com.lambda.spark.utils;

import java.io.File;
import java.text.MessageFormat;

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
        System.out.println("dir: " + dirname + " " + (file.exists() ? "exists" : " not exists"));
        file = new File(dirname);
        if (!file.isDirectory()) {
            throw new Exception("only support dir, given dir:" + dirname);
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
        // file.mkdir();
        Runtime runtime = Runtime.getRuntime();
        runtime.exec(MessageFormat.format("mkdir {0}", dirname));
    }
}

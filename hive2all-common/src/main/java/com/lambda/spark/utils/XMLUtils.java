package com.lambda.spark.utils;

import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.io.FileFilter;
import java.util.*;

/**
 * @Author: zhangxinsen
 * @Date: 2022/1/20 10:35 PM
 * @Desc:
 * @Version: v1.0
 */

public class XMLUtils {

    /**
     * @param filename 配置文件名
     * @return
     */
    public static Map<String, String> getConfiguration(String filename, Map<String, String> configuration) {
        SAXReader reader = new SAXReader();
        try {
            // 通过reader对象的read方法加载books.xml文件,获取document对象。
            Document document = reader.read(new File(filename));
            // 通过document对象获取根节点bookstore
            Element configurationRoot = document.getRootElement();
            // 通过element对象的elementIterator方法获取迭代器
            Iterator it = configurationRoot.elementIterator();
            // 遍历迭代器，获取根节点中的信息
            while (it.hasNext()) {
                Element property = (Element) it.next();
                List<Attribute> attributes = property.attributes();
                for (Attribute attr : attributes) {
                    String name = attr.getName();
                    String value = attr.getValue();
                    configuration.put(name, value);
                }

                Iterator itt = property.elementIterator();
                String key = null;
                String value;
                while (itt.hasNext()) {
                    Element child = (Element) itt.next();
                    if (child.getName().equals("name")) {
                        key = child.getStringValue();
                    }
                    if (child.getName().equals("value")) {
                        value = child.getStringValue();
                        configuration.put(key, value);
                    }
                }
            }
        } catch (DocumentException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return configuration;
    }

    public static Map<String, String> getConfigurationInDir(String dirname) {
        Map<String, String> configurations = new HashMap<>();
        File file = new File(dirname);
        File[] files = file.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                if (pathname.isDirectory()) {
                    return false;
                }
                if (!pathname.getName().endsWith(".xml")) {
                    return false;
                }

                if (!pathname.canRead()) {
                    return false;
                }
                return true;
            }
        });
        for (File file1 : files) {
            getConfiguration(file1.getAbsolutePath(), configurations);
        }
        return configurations;
    }


    /**
     * {where: {dt=20200212, dt=20200216}, having: {abc}}
     *
     * @param filename
     * @param configuration
     * @param tableName
     * @return
     */
    public static Map<String, List<String>> getFilterConditions(String filename, Map<String, String> configuration, String tableName) {
        Map<String, List<String>> result = new HashMap<>();
        String key = null;
        for (Map.Entry<String, String> stringStringEntry : configuration.entrySet()) {
            if (stringStringEntry.getValue().equals(tableName)) {
                key = stringStringEntry.getKey();
                break;
            }
        }
        if (key == null) {
            return null;
        }


        SAXReader reader = new SAXReader();
        try {
            // 通过reader对象的read方法加载books.xml文件,获取document对象。
            Document document = reader.read(new File(filename));
            // 通过document对象获取根节点bookstore
            Element configurationRoot = document.getRootElement();
            // 通过element对象的elementIterator方法获取迭代器
            Iterator it = configurationRoot.elementIterator();
            // 遍历迭代器，获取根节点中的信息
            while (it.hasNext()) {
                Element property = (Element) it.next();
                List<Attribute> attributes = property.attributes();
                for (Attribute attr : attributes) {
                    String name = attr.getName();
                    String value = attr.getValue();
                    configuration.put(name, value);
                }

                Iterator itt = property.elementIterator();
                List<List<String>> kvList = new LinkedList<>();
                while (itt.hasNext()) {
                    Element child = (Element) itt.next();
                    List<String> temp = new LinkedList<>();
                    temp.add(child.getName());
                    temp.add(child.getStringValue());
                    kvList.add(temp);
                }
                String configNodeTableNameKey = null;
                String configNodeTableNameValue = null;
                for (List<String> stringList : kvList) {
                    String s = stringList.get(0);
                    if (s.equals("name")) {
                        configNodeTableNameKey = stringList.get(1);
                    }
                    if (s.equals("value")) {
                        configNodeTableNameValue = stringList.get(1);
                    }
                }
                if (configNodeTableNameKey == null) {
                    return null;
                }
                if (configNodeTableNameValue == null) {
                    return null;
                }
                if (configNodeTableNameKey.equals(key) && configNodeTableNameValue.equals(tableName)) {
                    for (List<String> strings : kvList) {
                        // System.out.println("Key: " + strings.get(0) + ", value: " + strings.get(1));
                        String nodeKey = strings.get(0);
                        String nodeValue = strings.get(1);
                        if (nodeKey.equals("name") || nodeKey.equals("value")) {
                            continue;
                        }
                        if (nodeKey.contains("condition")) {
                            String clauseKey = nodeKey.split("-")[1];
                            List<String> temp = result.get(clauseKey);
                            if (temp == null) {
                                temp = new LinkedList<>();
                            }
                            temp.add(nodeValue);
                            result.put(clauseKey, temp);
                        }
                    }
                }
            }
        } catch (DocumentException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }

    public static Map<String, List<String>> getFilterConditionsInDir(String dirname, Map<String, String> configuration, String tableName) {
        Map<String, List<String>> result = new HashMap<>();
        File file = new File(dirname);
        for (File listFile : file.listFiles()) {
            Map<String, List<String>> filterConditions = getFilterConditions(listFile.getAbsolutePath(),
                    configuration,
                    tableName);
            if (filterConditions == null) {
                continue;
            }
            for (Map.Entry<String, List<String>> stringListEntry : filterConditions.entrySet()) {
                String clauseKey = stringListEntry.getKey();
                List<String> conditionList = stringListEntry.getValue();
                List<String> temp = result.get(clauseKey);
                if (temp == null) {
                    temp = new LinkedList<>();
                }
                for (String condition : conditionList) {
                    temp.add(condition);
                }
                result.put(clauseKey, temp);
            }
        }
        return result;
    }

    /**
     * 通过SQL的别名获得SQL的内容
     *
     * @param configuration
     * @param sqlName
     * @return
     */
    private static String getSql(Map<String, String> configuration, String sqlName) {
        String sql = configuration.get(sqlName);
        return sql;
    }

    /**
     * 通过SQL的开关来确定SQL是否正常使用
     * 每一个页签包涵4个属性
     * sql.name: sql别名
     * sql.value: sql表达式
     * sql.enable: 是否启用
     * sql.description: SQL的注释
     * @return : List<Map<String, String>>
     *     Map: {
     *         "name": "临时SQL",
     *         "value": " select * from temp",
     *         "isSqlEnable": false,
     *         "description": "测试"
     *     }
     */
    private static List<Map<String, String>> getSqlConfig(String sqlFileName) {
        List<Map<String, String>> result = new LinkedList<>();
        SAXReader reader = new SAXReader();
        try {
            // 通过reader对象的read方法加载books.xml文件,获取document对象。
            Document document = reader.read(new File(sqlFileName));
            // 通过document对象获取根节点bookstore
            Element configurationRoot = document.getRootElement();
            // 通过element对象的elementIterator方法获取迭代器
            Iterator it = configurationRoot.elementIterator();
            // 遍历迭代器，获取根节点中的信息
            while (it.hasNext()) {
                Element property = (Element) it.next();

                Iterator<Element> itt = property.elementIterator();
                Map<String, String> temp = new HashMap<>();
                while (itt.hasNext()) {
                    Element child = (Element) itt.next();
                    if (child.getName().equals("sql.name")) {
                        temp.put("name", child.getStringValue());
                    }
                    if (child.getName().equals("sql.value")) {
                        temp.put("value", child.getStringValue());
                    }
                    if (child.getName().equals("sql.enable")) {
                        temp.put("isSqlEnable", child.getStringValue());
                    }
                    if (child.getName().equals("sql.description")) {
                        temp.put("description", child.getStringValue());
                    }
                }
                if (temp.get("name") != null && temp.get("value") != null) {
                    Map<String, String> copySQL = new HashMap<>();
                    copySQL.put("name", temp.get("name"));
                    copySQL.put("value", temp.get("value"));
                    if (temp.get("isSqlEnable") != null && temp.get("isSqlEnable").equals("false")) {
                        copySQL.put("isSqlEnable", "false");
                    } else {
                        copySQL.put("isSqlEnable", "true");
                    }
                    copySQL.put("description", temp.get("description"));
                    result.add(copySQL);
                }
            }
        } catch (DocumentException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }

    public static List<Map<String, String>> getSqlConfigInDir(String dirname) {
        Map<String, String> configuration = getConfigurationInDir(dirname);
        String sqlFilePath = configuration.get("sql.file");
        if (sqlFilePath == null) {
            return null;
        }
        return getSqlConfig(sqlFilePath);
    }

//    public static void main(String[] args) {
//        List<Map<String, String>> sqlConfigInDir = getSqlConfigInDir("/Users/zhangxinsen/workspace/hive2all/config");
//        for (Map<String, String> stringStringMap : sqlConfigInDir) {
//            String sqlName = stringStringMap.get("name");
//            String sqlValue = stringStringMap.get("value");
//            boolean isSqlEnable = stringStringMap.get("isSqlEnable").equals("true");
//            System.out.println(
//                    "sql name: " + sqlName + ", value: " + sqlValue + ", isEnable: " + (isSqlEnable ? "true" : "false")
//            );
//        }
//    }
}

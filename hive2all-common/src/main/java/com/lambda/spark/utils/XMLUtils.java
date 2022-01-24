package com.lambda.spark.utils;

import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.io.FileFilter;
import java.util.*;

import static com.lambda.spark.utils.ConfigUtils.getTableLists;

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

//    public static void main(String[] args) {
//        String taskConfigDir = "/Users/zhangxinsen/workspace/hive2all/config/";
//        Map<String, String> configuration = getConfigurationInDir(taskConfigDir);
//        List<String> tableLists = getTableLists(configuration);
//        for (String tableName : tableLists) {
//            System.out.println("======= " + tableName + " =========");
//            Map<String, List<String>> filterConditions = getFilterConditionsInDir(taskConfigDir, configuration, tableName);
//            for (Map.Entry<String, List<String>> stringListEntry : filterConditions.entrySet()) {
//                System.out.println(stringListEntry.getKey());
//                for (String condition : stringListEntry.getValue()) {
//                    System.out.println(condition);
//                }
//            }
//        }
//    }
}

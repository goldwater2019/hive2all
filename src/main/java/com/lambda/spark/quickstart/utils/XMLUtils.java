package com.lambda.spark.quickstart.utils;

import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.io.FileFilter;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
}

package com.atguigu.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyUtil {

    public static Properties properties = null;

    static {
        try {
            InputStream is = ClassLoader.getSystemResourceAsStream("kafka.properties");
            properties = new Properties();
            properties.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //获取指定配置信息
    public static String getProperty(String key) {
        return properties.getProperty(key);
    }

}

package hualala.com.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 这个类主要就是用户管理整个项目中用到的配置信息，根据用户的选择，加载不同的配置
 *
 * 一般情况下这种配置文件的类型，要么单例，要不不提供对象，所有函数都是静态
 */
public class ConfigurationManager {
    private ConfigurationManager(){}
    private static Properties properties;
    static {
        try {
            properties = new Properties();
            InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("conf.properties");
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static int getIntegerProperty(String key) {
       return  Integer.valueOf(properties.getProperty(key, "0"));
    }

    public static Long getLongProperty(String key) {
        return  Long.valueOf(properties.getProperty(key, "0"));
    }

    public static String getProperty(String key) {
        return  properties.getProperty(key);
    }

    public static boolean getBooleanProperty(String key) {
        return  Boolean.valueOf(properties.getProperty(key, "true"));
    }
    public static double getDoubleProperty(String key) {
        return  Double.valueOf(properties.getProperty(key, "0.0"));
    }
}

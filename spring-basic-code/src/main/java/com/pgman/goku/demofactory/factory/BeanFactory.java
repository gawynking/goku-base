package com.pgman.goku.demofactory.factory;

import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class BeanFactory {

    private static Properties properties;
    private static Map<String,Object> beans;

    // 单例模式初始化
    static{
        try {
            properties = new Properties();
            // 1 加载配置文件
            InputStream inputStream = BeanFactory.class.getClassLoader().getResourceAsStream("bean.properties");
            properties.load(inputStream);
            beans = new HashMap<String,Object>();
            // 2 获取配置文件key值集合
            Enumeration keys = properties.keys();
            // 3 遍历集合，初始化对象并保存到容器
            while (keys.hasMoreElements()){
                String key = keys.nextElement().toString();
                String beanPath = properties.getProperty(key);
                Object value = Class.forName(beanPath).newInstance();
                beans.put(key,value);
            }

        }catch (Exception e){
            e.printStackTrace();
        }

    }

    // 根据配置文件name获取对象
    public static Object getBean(String beanName){
        return beans.get(beanName);
    }
}

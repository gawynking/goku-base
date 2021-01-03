package com.pgman.goku.listener;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

/**
 Listener：监听器
     * 概念：web的三大组件之一。
     * 事件监听机制
     * 事件	：一件事情
     * 事件源 ：事件发生的地方
     * 监听器 ：一个对象
     * 注册监听：将事件、事件源、监听器绑定在一起。 当事件源上发生某个事件后，执行监听器代码


 * ServletContextListener:监听ServletContext对象的创建和销毁
     * 方法：
         * void contextDestroyed(ServletContextEvent sce) ：ServletContext对象被销毁之前会调用该方法
         * void contextInitialized(ServletContextEvent sce) ：ServletContext对象创建后会调用该方法
 */
@WebListener
public class ContentLoaderAnnoListener implements ServletContextListener {

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
//        加载资源文件
        System.out.println("ContentLoaderAnnoListener contextInitialized 创建");
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        System.out.println("ContentLoaderAnnoListener contextDestroyed 销毁");

    }

}

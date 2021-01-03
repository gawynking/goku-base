package com.pgman.goku.servlet;

import javax.servlet.*;
import java.io.IOException;

/**
 * 类开发好以后需要再web.xml中配置servlet映射
 *
 * ## Servlet：
 * 1. 概念
 * 2. 步骤
 * 3. 执行原理
 * 4. 生命周期
 * 5. Servlet3.0 注解配置
 * 6. Servlet的体系结构
 * Servlet -- 接口
 * |
 * GenericServlet -- 抽象类
 * |
 * HttpServlet  -- 抽象类
 *
 * GenericServlet：将Servlet接口中其他的方法做了默认空实现，只将service()方法作为抽象
 * 将来定义Servlet类时，可以继承GenericServlet，实现service()方法即可
 *
 * HttpServlet：对http协议的一种封装，简化操作
 * 1. 定义类继承HttpServlet
 * 2. 复写doGet/doPost方法
 *
 * 7. Servlet相关配置
 * 1. urlpartten:Servlet访问路径
 * 1. 一个Servlet可以定义多个访问路径 ： @WebServlet({"/d4","/dd4","/ddd4"})
 * 2. 路径定义规则：
 * 1. /xxx：路径匹配
 * 2. /xxx/xxx:多层路径，目录结构
 * 3. *.do：扩展名匹配
 */
public class ServletDemo implements Servlet{

    /**
     * 初始化方法 只在加载时执行一次，说明默认情况下servlet是单例模式
     *
     * 在多线程访问模式下，存在线程安全问题
     * 解决方法：不要在servlet中定义成员变量 ，如果定了了变量 一般也不要对其进行更改操作
     *
     * @param servletConfig
     * @throws ServletException
     */
    @Override
    public void init(ServletConfig servletConfig) throws ServletException {

    }

    @Override
    public ServletConfig getServletConfig() {
        return null;
    }

    /**
     * 提供服务的方法 ,每次请求都执行
     *
     * @param servletRequest
     * @param servletResponse
     * @throws ServletException
     * @throws IOException
     */
    @Override
    public void service(ServletRequest servletRequest, ServletResponse servletResponse) throws ServletException, IOException {

        System.out.println("hello servlet demo 01!");

    }

    @Override
    public String getServletInfo() {
        return null;
    }

    /**
     * 销毁方法，在服务器关闭时执行
     *
     */
    @Override
    public void destroy() {

    }

}

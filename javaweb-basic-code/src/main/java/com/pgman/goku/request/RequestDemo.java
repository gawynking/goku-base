package com.pgman.goku.request;


import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Enumeration;

@WebServlet("/requestDemo")
public class RequestDemo extends HttpServlet{

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        /**
         *
         * 1. 获取请求方式 ：GET
         * String getMethod()
         *
         * 2. (*)获取虚拟目录：/day14
         * String getContextPath()
         *
         * 3. 获取Servlet路径: /demo1
         * String getServletPath()
         *
         * 4. 获取get方式请求参数：name=zhangsan
         * String getQueryString()
         *
         * 5. (*)获取请求URI：/day14/demo1
         * String getRequestURI():		/day14/demo1
         * StringBuffer getRequestURL()  :http://localhost/day14/demo1
         *
         * URL:统一资源定位符 ： http://localhost/day14/demo1	中华人民共和国
         * URI：统一资源标识符 : /day14/demo1					共和国
         *
         * 6. 获取协议及版本：HTTP/1.1
         * String getProtocol()
         *
         * 7. 获取客户机的IP地址：
         * String getRemoteAddr()
         *
         * 8. 获取请求头数据
         * 方法：
         * (*)String getHeader(String name):通过请求头的名称获取请求头的值
         * Enumeration<String> getHeaderNames():获取所有的请求头名称
         *
         */


        // 1. 获取请求方式 ：GET
        String method = req.getMethod();
        System.out.println(method);

        // 2. (*)获取虚拟目录：/day14
        String contextPath = req.getContextPath();
        System.out.println(contextPath);

        // 3. 获取Servlet路径: /demo1
        String servletPath = req.getServletPath();
        System.out.println(servletPath);

        // 4. 获取get方式请求参数：name=zhangsan
        String queryString = req.getQueryString();
        System.out.println(queryString);

        // 5. (*)获取请求URI：/day14/demo1
        String requestURI = req.getRequestURI();
        StringBuffer requestURL = req.getRequestURL();

        System.out.println(requestURI);
        System.out.println(requestURL);

        // 6. 获取协议及版本：HTTP/1.1
        String protocol = req.getProtocol();
        System.out.println(protocol);

        //  7. 获取客户机的IP地址：
        String remoteAddr = req.getRemoteAddr();
        System.out.println(remoteAddr);


        //  8. 获取请求头数据
        Enumeration<String> headerNames = req.getHeaderNames();
        while (headerNames.hasMoreElements()){
            String name = headerNames.nextElement();
            String value = req.getHeader(name);

            System.out.println(name + " --> " + value);
        }



    }


    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        /**
         * 3. 获取请求体数据:
         * 请求体：只有POST请求方式，才有请求体，在请求体中封装了POST请求的请求参数
         * 步骤：
         * 1. 获取流对象
         *  BufferedReader getReader()：获取字符输入流，只能操作字符数据
         *  ServletInputStream getInputStream()：获取字节输入流，可以操作所有类型数据
         */

        BufferedReader reader = req.getReader();
        String line = null;
        while ((line = reader.readLine()) != null){
            System.out.println(line);
        }


    }

}

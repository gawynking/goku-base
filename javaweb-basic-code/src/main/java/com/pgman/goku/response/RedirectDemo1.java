package com.pgman.goku.response;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * 重定向
 * 重定向：资源跳转的方式

 * forward 和  redirect 区别
 * 重定向的特点:redirect
 1. 地址栏发生变化
 2. 重定向可以访问其他站点(服务器)的资源
 3. 重定向是两次请求。不能使用request对象来共享数据
 * 转发的特点：forward
 1. 转发地址栏路径不变
 2. 转发只能访问当前服务器下的资源
 3. 转发是一次请求，可以使用request对象来共享数据
 */
@WebServlet("/redirectDemo1")
public class RedirectDemo1 extends HttpServlet{

    /**
     * ## Response对象
     * 功能：设置响应消息
     1. 设置响应行
     1. 格式：HTTP/1.1 200 ok
     2. 设置状态码：setStatus(int sc)
     2. 设置响应头：setHeader(String name, String value)

     3. 设置响应体：
     * 使用步骤：
     1. 获取输出流
     * 字符输出流：PrintWriter getWriter()

     * 字节输出流：ServletOutputStream getOutputStream()

     2. 使用输出流，将数据输出到客户端浏览器

     * @param req
     * @param resp
     * @throws ServletException
     * @throws IOException
     */
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        System.out.println("RedirectDemo1");

        // 实现方式1
//        resp.setStatus(302);
//        resp.setHeader("location","/redirectDemo2");

        // 实现方式2
        resp.sendRedirect("/redirectDemo2");

    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        this.doGet(req,resp);
    }


}

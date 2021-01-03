package com.pgman.goku.context;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * 域对象：共享数据
 1. setAttribute(String name,Object value)
 2. getAttribute(String name)
 3. removeAttribute(String name)

 * ServletContext对象范围：所有用户所有请求的数据
 */
@WebServlet("/servletContextApplicationDataDemo1")
public class ServletContextApplicationDataDemo1 extends HttpServlet{

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        resp.setContentType("text/html;charset=utf-8");


        // 1 获取ServletContext
        ServletContext context = this.getServletContext();

        context.setAttribute("msg","设置数据");


    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        this.doGet(req,resp);
    }
}

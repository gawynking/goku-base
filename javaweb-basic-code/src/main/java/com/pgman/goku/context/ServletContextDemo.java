package com.pgman.goku.context;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * ServletContext 代表整个web应用，可以和程序的容器(服务器)来通信
 */
@WebServlet("/servletContextDemo")
public class ServletContextDemo extends HttpServlet{

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        // 1 获取ServletContext的两种方式
        ServletContext servletContext1 = this.getServletContext();
        ServletContext servletContext2 = req.getServletContext();

        System.out.println(servletContext1);
        System.out.println(servletContext2);

        System.out.println(servletContext1 == servletContext2);

    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        this.doGet(req,resp);
    }
}

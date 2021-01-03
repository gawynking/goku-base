package com.pgman.goku.servlet;


import javax.servlet.GenericServlet;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebServlet;
import java.io.IOException;

/**
 * GenericServlet：将Servlet接口中其他的方法做了默认空实现，只将service()方法作为抽象
 * 将来定义Servlet类时，可以继承GenericServlet，实现service()方法即可
 */
@WebServlet("/genericServletDemo")
public class GenericServletDemo extends GenericServlet{
    @Override
    public void service(ServletRequest servletRequest, ServletResponse servletResponse) throws ServletException, IOException {
        System.out.println("继承GenericServlet实现");
    }
}

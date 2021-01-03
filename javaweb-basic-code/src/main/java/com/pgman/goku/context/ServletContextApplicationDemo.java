package com.pgman.goku.context;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;


@WebServlet("/servletContextApplicationDemo")
public class ServletContextApplicationDemo extends HttpServlet{

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        resp.setContentType("text/html;charset=utf-8");


        // 1 获取ServletContext
        ServletContext context = this.getServletContext();

        /**
         * 获取MIME类型：
         * MIME类型:在互联网通信过程中定义的一种文件数据类型
         * 格式： 大类型/小类型   text/html		image/jpeg

         * 获取：String getMimeType(String file)
         */
        String fileName = "a.jpg";
        String mimeType = context.getMimeType(fileName);
        System.out.println(mimeType);



        /**
         * 获取文件的真实(服务器)路径
         *  1. 方法：String getRealPath(String path)
         */
        String realPath = context.getRealPath("/login.html");
        System.out.println(realPath);



    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        this.doGet(req,resp);
    }
}

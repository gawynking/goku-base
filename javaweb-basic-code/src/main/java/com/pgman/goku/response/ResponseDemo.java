package com.pgman.goku.response;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

@WebServlet("/responseDemo")
public class ResponseDemo extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        // 1.1 设置字符集
//        resp.setCharacterEncoding("utf-8");
//        resp.setHeader("content-type","text/html;charset=utf-8");

        // 1.2 设置字符编码方式2 简单方式
        resp.setContentType("text/html;charset=utf-8");


        // 2 获取字符输出流
//        PrintWriter writer = resp.getWriter();
//        writer.write("我是pgman,字符输出");


        // 3 字节输出流
        ServletOutputStream os = resp.getOutputStream();
        os.write("我是pgman,字节输出".getBytes("utf-8"));

    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        this.doGet(req,resp);
    }

}

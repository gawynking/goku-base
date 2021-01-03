package com.pgman.goku.cookie;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URLDecoder;

/**
 * ## Cookie：
 概念：客户端会话技术，将数据保存到客户端，由request获取cookie数据，每次请求携带cookie
 */
@WebServlet("/cookieDemo2")
public class CookieDemo2 extends HttpServlet{

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        Cookie[] cookies = req.getCookies();
        if(cookies != null) {
            for (Cookie cookie : cookies) {
                String name = cookie.getName();
                String value = URLDecoder.decode(cookie.getValue(),"utf-8");

                System.out.println(name + ":" + value);
            }
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        this.doGet(req,resp);

    }


}

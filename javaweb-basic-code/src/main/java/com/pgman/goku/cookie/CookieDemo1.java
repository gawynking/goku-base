package com.pgman.goku.cookie;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URLEncoder;

/**
 Cookie：
     1. 概念：客户端会话技术，将数据保存到客户端，每次请求携带cookie,由服务器端设置保存到贡献域

     2. 使用步骤：
         1. 创建Cookie对象，绑定数据
              new Cookie(String name, String value)
         2. 发送Cookie对象
              response.addCookie(Cookie cookie)
         3. 获取Cookie，拿到数据
              Cookie[]  request.getCookies()

     3. 实现原理
         基于响应头set-cookie和请求头cookie实现

    4. cookie的细节
         1. 一次可不可以发送多个cookie?
             * 可以
             * 可以创建多个Cookie对象，使用response调用多次addCookie方法发送cookie即可。

         2. cookie在浏览器中保存多长时间？
             1. 默认情况下，当浏览器关闭后，Cookie数据被销毁
             2. 持久化存储：
             * setMaxAge(int seconds)
                 1. 正数：将Cookie数据写到硬盘的文件中。持久化存储。并指定cookie存活时间，时间到后，cookie文件自动失效
                 2. 负数：默认值
                 3. 零：删除cookie信息
         3. cookie能不能存中文？
             * 在tomcat 8 之前 cookie中不能直接存储中文数据。
             * 需要将中文数据转码---一般采用URL编码(%E3)
             * 在tomcat 8 之后，cookie支持中文数据。特殊字符还是不支持，建议使用URL编码存储，URL解码解析

         4. cookie共享问题？
             1. 假设在一个tomcat服务器中，部署了多个web项目，那么在这些web项目中cookie能不能共享？
                 * 默认情况下cookie不能共享

             * setPath(String path):设置cookie的获取范围。默认情况下，设置当前的虚拟目录
                 * 如果要共享，则可以将path设置为"/"


             2. 不同的tomcat服务器间cookie共享问题？
                 * setDomain(String path):如果设置一级域名相同，那么多个服务器之间cookie可以共享
                 * setDomain(".baidu.com"),那么tieba.baidu.com和news.baidu.com中cookie可以共享


         5. Cookie的特点和作用
             1. cookie存储数据在客户端浏览器
             2. 浏览器对于单个cookie 的大小有限制(4kb) 以及 对同一个域名下的总cookie数量也有限制(20个)

             * 作用：
                 1. cookie一般用于存出少量的不太敏感的数据
                 2. 在不登录的情况下，完成服务器对客户端的身份识别
 */
@WebServlet("/cookieDemo1")
public class CookieDemo1 extends HttpServlet{

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        resp.setContentType("text/html;charset=utf-8");

        // 1 创建cookie
        Cookie cookie1 = new Cookie("msg", "hello");
        String value = URLEncoder.encode("张三","utf-8");
        Cookie cookie2 = new Cookie("name", value);

        // 2 设置过期时间
        cookie1.setMaxAge(30);
        cookie2.setMaxAge(60);

        // 3 存储cookie
        resp.addCookie(cookie1);
        resp.addCookie(cookie2);

    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        this.doGet(req,resp);

    }


}

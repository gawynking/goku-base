package com.pgman.goku.request;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Map;
import java.util.Set;

@WebServlet("/genericRequestDemo")
public class GenericRequestDemo extends HttpServlet {

    /**
     * 获取参数值得几种方式
     *
     * @param req
     * @param resp
     * @throws ServletException
     * @throws IOException
     */
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        /**
         * 中文乱码问题：
         * get方式：tomcat 8 已经将get方式乱码问题解决了
         * post方式：会乱码
         * 解决：在获取参数前，设置request的编码request.setCharacterEncoding("utf-8");
         *
         */

        req.setCharacterEncoding("utf-8");


        /**
         * 一. 获取请求参数通用方式：不论get还是post请求方式都可以使用下列方法来获取请求参数
         *     1. String getParameter(String name):根据参数名称获取参数值    username=zs&password=123
         *     2. String[] getParameterValues(String name):根据参数名称获取参数值的数组  hobby=xx&hobby=game
         *     3. Enumeration<String> getParameterNames():获取所有请求的参数名称
         *     4. Map<String,String[]> getParameterMap():获取所有参数的map集合
         */
        String username = req.getParameter("username");
        System.out.println(username);

        String[] games = req.getParameterValues("game");
        for(String game :games){
            System.out.println(game);
        }

        Enumeration<String> parameterNames = req.getParameterNames();
        while (parameterNames.hasMoreElements()){
            String element = parameterNames.nextElement();
            System.out.println(element);
        }

        Map<String, String[]> parameterMap = req.getParameterMap();
        Set<String> keySet = parameterMap.keySet();
        for(String key :keySet){

            String[] values = parameterMap.get(key);

            for(String value :values){
                System.out.println(value);
            }

            System.out.println("---------------------------------------");
        }

    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        this.doGet(req,resp);

    }


}

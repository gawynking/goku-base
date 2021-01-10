package com.pgman.travel.web.servlet;

import com.pgman.travel.domain.ResultInfo;
import com.pgman.travel.domain.User;
import com.pgman.travel.service.UserService;
import com.pgman.travel.service.impl.UserServiceImpl;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.beanutils.BeanUtils;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;


@WebServlet("/user/*")
public class UserServlet extends BaseServlet {

    private UserService userService = new UserServiceImpl();


    /**
     * 注册方法
     *
     * @param req
     * @param resp
     * @throws ServletException
     * @throws IOException
     */
    public void regist(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        // 定义返回值
        ResultInfo info = null;

        // 处理验证码
        String check1 = req.getParameter("check");
        HttpSession session = req.getSession();
        String check2 = (String) session.getAttribute("CHECKCODE_SERVER");
        session.removeAttribute("CHECKCODE_SERVER");

        if (check2 == null || !check2.equalsIgnoreCase(check1)) {

            info = new ResultInfo();

            info.setFlag(false);
            info.setErrorMsg("验证码错误!");

            // jackjson api
            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writeValueAsString(info);

            resp.setContentType("application/json;charset=utf-8");
            resp.getWriter().write(json);

            return;
        }


        // 封装前端数据
        Map<String, String[]> parameterMap = req.getParameterMap();
        User user = new User();
        try {
            BeanUtils.populate(user, parameterMap);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }

        // 调用service 完成注册用户
        boolean flag = userService.regist(user);

        if (flag) {
            info = new ResultInfo();
            info.setFlag(true);
        } else {
            info = new ResultInfo();
            info.setFlag(false);
            info.setErrorMsg("注册失败!");
        }

        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(info);

        resp.setContentType("application/json;charset=utf-8");
        resp.getWriter().write(json);

    }


    /**
     * 查找用户
     *
     * @param req
     * @param resp
     * @throws ServletException
     * @throws IOException
     */
    public void findUser(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        Object user = req.getSession().getAttribute("user");
        ObjectMapper mapper = new ObjectMapper();
        resp.setContentType("application/json;charset=utf-8");
        mapper.writeValue(resp.getOutputStream(),user);

    }


    /**
     * 激活用户
     *
     * @param req
     * @param resp
     * @throws ServletException
     * @throws IOException
     */
    public void active(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        String code = req.getParameter("code");

        if(code != null) {
            boolean flag = userService.active(code);
            String msg = null;
            if (flag) {
                msg = "激活成功，请<a href='login.html'>登录</a>";
            } else {
                msg = "激活失败，请联系管理员!";
            }

            resp.setContentType("text/html;charset=utf-8");
            resp.getWriter().write(msg);
        }

    }


    /**
     * 登录用户
     *
     * @param req
     * @param resp
     * @throws ServletException
     * @throws IOException
     */
    public void login(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {


        Map<String, String[]> parameterMap = req.getParameterMap();
        User user = new User();
        try {
            BeanUtils.populate(user,parameterMap);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }

        User u = userService.login(user);

        ResultInfo info = new ResultInfo();

        if(u == null){
            info.setFlag(false);
            info.setErrorMsg("用户名或密码错误!");
        }

        if(u != null && !"Y".equals(u.getStatus())){
            info.setFlag(false);
            info.setErrorMsg("您尚未激活，请登录邮箱激活!");
        }

        if(u != null && "Y".equals(u.getStatus())){
            // 登录成功后 将用户信息缓存
            req.getSession().setAttribute("user",u);
            info.setFlag(true);
        }

        //响应数据
        ObjectMapper mapper = new ObjectMapper();

        resp.setContentType("application/json;charset=utf-8");
        mapper.writeValue(resp.getOutputStream(),info);

    }


    /**
     * 退出用户
     *
     * @param req
     * @param resp
     * @throws ServletException
     * @throws IOException
     */
    public void exit(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        // 登录session中存在用户信息 ，退出就是删除session中的用户信息
        // 销毁session信息
        req.getSession().invalidate();

        // 重定向到登录界面
        resp.sendRedirect(req.getContextPath()+"/login.html");

    }



}

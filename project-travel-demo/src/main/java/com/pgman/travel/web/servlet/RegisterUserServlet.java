package com.pgman.travel.web.servlet;

import com.pgman.travel.domain.ResultInfo;
import com.pgman.travel.domain.User;
import com.pgman.travel.service.UserService;
import com.pgman.travel.service.impl.UserServiceImpl;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.beanutils.BeanUtils;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

@WebServlet("/registerUser")
public class RegisterUserServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

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
        UserService userService = new UserServiceImpl();
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


    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        this.doGet(req, resp);
    }


}

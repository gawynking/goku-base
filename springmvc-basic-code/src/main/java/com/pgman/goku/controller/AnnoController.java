package com.pgman.goku.controller;

import com.pgman.goku.domain.User;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.bind.support.SessionStatus;

import java.util.Date;
import java.util.Map;

@Controller
@RequestMapping("/anno")
@SessionAttributes(value={"msg"})   // 把msg=xxx存入到session域对中
public class AnnoController {

    /**
     * RequestParam注解 ：解决传入参数与javabean参数不一致问题
     * @param username
     * @return
     */
    @RequestMapping("/testRequestParam")
    public String testRequestParam(@RequestParam("name") String username){
        System.out.println(username);
        return "success";
    }

    /**
     * RequestBody注解：获取请求体内容
     * @param body
     * @return
     */
    @RequestMapping("/testRequestBody")
    public String testRequestBody(@RequestBody String body){
        System.out.println(body);
        return "success";
    }

    /**
     * PathVariable注解：rustful编程风格，用于获取传入的ID值
     * @param id
     * @return
     */
    @RequestMapping("/testPathVariable/{sid}")
    public String testPathVariable(@PathVariable(name="sid") String id){
        System.out.println(id);
        return "success";
    }

    /**
     * RequestHeader注解：获取请求头信息
     * @param header
     * @return
     */
    @RequestMapping("/testRequestHeader")
    public String testRequestHeader(@RequestHeader(value = "Accept") String header){
        System.out.println(header);
        return "success";
    }


    /**
     * CookieValue注解:获取cookie值信息
     * @param cookieValue
     * @return
     */
    @RequestMapping("/testCookieValue")
    public String testCookieValue(@CookieValue(value="JSESSIONID") String cookieValue){
        System.out.println(cookieValue);
        return "success";
    }

    /**
     * ModelAttribute注解：丰富数据
     * @param user
     * @return
     */
//    @RequestMapping("/testModelAttribute")
//    public String testModelAttribute(User user){
//        System.out.println("ModelAttribute执行了");
//        System.out.println(user);
//        return "success";
//    }

//    @ModelAttribute
//    public User testBeforeModelAttribute(String name,int age){
//        User user = new User();
//        user.setName(name);
//        user.setAge(age);
//        user.setHiredate(new Date());
//        return user;
//    }

    /**
     * ModelAttribute注解：无返回值，用Map结构封装结果集
     * @param user
     * @return
     */
    @RequestMapping("/testModelAttribute")
    public String testModelAttribute(@ModelAttribute("user") User user){
        System.out.println("ModelAttribute执行了");
        System.out.println(user);
        return "success";
    }

//    @ModelAttribute
//    public void testBeforeModelAttribute(String name, int age, Map<String,User> map){
//        User user = new User();
//        user.setName(name);
//        user.setAge(age);
//        user.setHiredate(new Date());
//
//        map.put("user",user);
//    }

    /**
     * SessionAttributes注解：实现数据交互
     * @param model
     * @return
     */
    @RequestMapping("/testSessionAttributes")
    public String testSessionAttributes(Model model){
        System.out.println("testSessionAttributes执行了");
        model.addAttribute("msg","SessionAttributes");
        return "success";
    }

    @RequestMapping("/getSessionAttributes")
    public String getSessionAttributes(ModelMap modelMap){
        System.out.println("getSessionAttributes执行了");
        String msg = (String) modelMap.get("msg");
        System.out.println(msg);
        return "success";
    }

    @RequestMapping("/delSessionAttributes")
    public String delSessionAttributes(SessionStatus sessionStatus){
        System.out.println("delSessionAttributes执行了");
        sessionStatus.setComplete();
        return "success";
    }

}

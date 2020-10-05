package com.pgman.goku.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
@RequestMapping(path = "/test")
public class HelloController {

    @RequestMapping(path = "/sayHello")
    public String sayHello(){
        System.out.println("Hello SpringMVC");
        return "success";
    }


    @RequestMapping(value = "/testRequstMapping",method = {RequestMethod.GET,RequestMethod.POST},params = {"username","password"},headers = {"Accept"})
    public String testRequstMapping(){
        System.out.println("测试testRequstMapping注解作用");
        return "success";
    }

}

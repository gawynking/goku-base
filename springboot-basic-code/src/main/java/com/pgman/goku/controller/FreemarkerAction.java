package com.pgman.goku.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

@Controller
@RequestMapping("/freemarker")
public class FreemarkerAction {


    @RequestMapping(value = "/toDemo")
    public ModelAndView toDemo(ModelAndView mv) {

        mv.addObject("name", "chavin");
        mv.setViewName("freemarker");

        return mv;

    }


}

package com.pgman.goku.controller;

import com.pgman.goku.domain.User;
import com.pgman.goku.exception.SysException;
import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;
import com.sun.xml.internal.ws.encoding.xml.XMLMessage;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.UUID;

@Controller
@RequestMapping("/user")
public class UserController {

    /**
     * 返回值String类型数据
     * @param model
     * @return
     */
    @RequestMapping("/testString")
    public String testString(Model model){
        System.out.println("testString执行了");

        User user = new User();
        user.setName("pgman");
        user.setAge(30);
        user.setHiredate(new Date());

        model.addAttribute("user",user);

        return "success";
    }

    /**
     * 返回值void类型
     * @param request
     * @param response
     * @throws Exception
     */
    @RequestMapping("/testVoid")
    public void testVoid(HttpServletRequest request, HttpServletResponse response) throws Exception{
        System.out.println("testVoid执行了");

        // 编写请求转发的程序
//         request.getRequestDispatcher("/WEB-INF/pages/success.jsp").forward(request,response);

        // 重定向
//         response.sendRedirect(request.getContextPath()+"/index.jsp");

        // 设置中文乱码
        response.setCharacterEncoding("UTF-8");
        response.setContentType("text/html;charset=UTF-8");

        // 直接会进行响应
        response.getWriter().print("你好");

         return;
    }

    /**
     * 返回ModelAndView
     * @return
     */
    @RequestMapping("/testModelAndView")
    public ModelAndView testModelAndView(){
        System.out.println("testModelAndView执行了");

        ModelAndView modelAndView = new ModelAndView();

        User user = new User();
        user.setName("pgman");
        user.setAge(30);
        user.setHiredate(new Date());

        modelAndView.addObject("user",user);
        modelAndView.setViewName("success");

        return modelAndView;
    }

    @RequestMapping("/testForwardOrRedirect")
    public String testForwardOrRedirect(){
        System.out.println("testForwardOrRedirect执行了");

        // 请求转发
//        return "forward:/WEB-INF/pages/success.jsp";

        // 重定向
        return "redirect:/index.jsp";
    }

    @RequestMapping("testAjax")
    public @ResponseBody User testAjax(@RequestBody User user){
        System.out.println("testAjax执行了");
        System.out.println(user);

        user.setName("chavinking");
        user.setAge(20);

        return user;
    }


    /**
     * 传统文件上传方法
     * @param request
     * @return
     * @throws Exception
     */
    @RequestMapping("fileupload1")
    public String fileuoload1(HttpServletRequest request) throws Exception {
        System.out.println("文件上传...");

        // 使用fileupload组件完成文件上传
        // 上传的位置
        String path = request.getSession().getServletContext().getRealPath("/uploads/");
        // 判断，该路径是否存在
        File file = new File(path);
        if(!file.exists()){
            // 创建该文件夹
            file.mkdirs();
        }

        // 解析request对象，获取上传文件项
        DiskFileItemFactory factory = new DiskFileItemFactory();
        ServletFileUpload upload = new ServletFileUpload(factory);
        // 解析request
        List<FileItem> items = upload.parseRequest(request);
        // 遍历
        for(FileItem item:items){
            // 进行判断，当前item对象是否是上传文件项
            if(item.isFormField()){
                // 说明普通表单向
            }else{
                // 说明上传文件项
                // 获取上传文件的名称
                String filename = item.getName();
                // 把文件的名称设置唯一值，uuid
                String uuid = UUID.randomUUID().toString().replace("-", "");
                filename = uuid+"_"+filename;
                // 完成文件上传
                item.write(new File(path,filename));
//                System.out.println(path+""+filename);
                // 删除临时文件
                item.delete();
            }
        }

        return "success";
    }

    /**
     * SpringMVC文件上传
     * @param request
     * @param upload
     * @return
     * @throws Exception
     */
    @RequestMapping("fileupload2")
    public String fileuoload2(HttpServletRequest request, MultipartFile upload) throws Exception {
        System.out.println("springmvc文件上传...");

        // 使用fileupload组件完成文件上传
        // 上传的位置
        String path = request.getSession().getServletContext().getRealPath("/uploads/");
        // 判断，该路径是否存在
        File file = new File(path);
        if(!file.exists()){
            // 创建该文件夹
            file.mkdirs();
        }

        // 说明上传文件项
        // 获取上传文件的名称
        String filename = upload.getOriginalFilename();
        // 把文件的名称设置唯一值，uuid
        String uuid = UUID.randomUUID().toString().replace("-", "");
        filename = uuid + "_" + filename;
        // 完成文件上传
        upload.transferTo(new File(path,filename));

        return "success";
    }

    /**
     * springMVC异常处理
     * @return
     * @throws SysException
     */
    @RequestMapping("testException")
    public String testException() throws SysException{
        System.out.println("testException执行了");

        try {
            float i = 10/0;
        } catch (Exception e) {
            e.printStackTrace();
            throw new SysException("程序运行出错了.");
        }

        return "success";
    }


    @RequestMapping("/testInterceptor")
    public String testInterceptor(){
        System.out.println("testInterceptor执行了.");
        return "success";
    }

}

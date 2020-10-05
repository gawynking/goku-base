package com.pgman.goku.controller;

import com.pgman.goku.dao.IAccountDao;
import com.pgman.goku.domain.Account;
import com.pgman.goku.service.IAccountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;

@Controller
@RequestMapping("/account")
public class AccountController {

    @Autowired
    private IAccountService accountService;


    /**
     * 测试springmvc
     * @return
     */
    @RequestMapping("/testFindAll")
    public String testFindAll(Model model){
        System.out.println("表现层：查询所有.");
        List<Account> accounts = accountService.findAll();
        model.addAttribute("accounts",accounts);
        return "list";
    }

    /**
     * 测试保存账户信息
     * @param account
     * @param request
     * @param response
     * @return
     */
    @RequestMapping("saveAccount")
    public void testAaveAccount(Account account, HttpServletRequest request, HttpServletResponse response) throws Exception{
        System.out.println("表现层：保存账户.");
        accountService.saveAccount(account);
        response.sendRedirect(request.getContextPath()+"/account/testFindAll");
        return;
    }

}

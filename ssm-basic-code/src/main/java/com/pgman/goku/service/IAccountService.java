package com.pgman.goku.service;

import com.pgman.goku.domain.Account;

import java.util.List;

public interface IAccountService {

    /**
     * 查询所有账户
     * @return
     */
    public List<Account> findAll();

    /**
     * 保存用户信息
     * @param account
     */
    public void saveAccount(Account account);


}

package com.pgman.goku.dao;

import com.pgman.goku.domain.Account;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository("accountDao")
public interface IAccountDao {

    /**
     * 查询所有账户
     * @return
     */
    @Select("select * from account")
    public List<Account> findAll();

    /**
     * 保存用户信息
     * @param account
     */
    @Insert("insert into account (name,money)values(#{name},#{money})")
    public void saveAccount(Account account);

}

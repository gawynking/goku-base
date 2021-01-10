package com.pgman.travel.dao;

import com.pgman.travel.domain.User;

public interface UserDao {


    /**
     * 保存用户信息
     *
     * @param user
     */
    void save(User user);


    /**
     * 根据username查询用户
     *
     * @param username
     */
    User findByUsername(String username);


    /**
     * 根据code查询用户
     *
     * @param code
     * @return
     */
    User findByCode(String code);

    /**
     * 更新status
     *
     * @param uid
     * @return
     */
    int updateStatus(int uid);

    /**
     * 根据用户名和密码查询用户
     *
     * @param username
     * @param password
     */
    User findByUsernameAndPassword(String username, String password);
}

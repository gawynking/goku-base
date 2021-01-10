package com.pgman.travel.service;

import com.pgman.travel.domain.User;

public interface UserService {

    /**
     * 注册用户
     *
     * @param user
     * @return
     */
    boolean regist(User user);


    /**
     * 激活用户
     *
     * @param code
     * @return
     */
    boolean active(String code);


    /**
     * 用户登录
     *
     * @param user
     * @return
     */
    User login(User user);
}

package com.pgman.travel.service.impl;

import com.pgman.travel.dao.UserDao;
import com.pgman.travel.domain.User;
import com.pgman.travel.service.UserService;
import com.pgman.travel.dao.impl.UserDaoImpl;
import com.pgman.travel.util.MailUtils;
import com.pgman.travel.util.UuidUtil;

public class UserServiceImpl implements UserService {

    private UserDao userDao = new UserDaoImpl();

    /**
     * 注册用户
     *
     * @param user
     * @return
     */
    @Override
    public boolean regist(User user) {

        // 查询用户信息
        User u = userDao.findByUsername(user.getUsername());
        if(u != null){
            return false;
        }

        //2.保存用户信息
        //2.1设置激活码，唯一字符串
        user.setCode(UuidUtil.getUuid());
        //2.2设置激活状态
        user.setStatus("N");

        userDao.save(user);

        //3.激活邮件发送，邮件正文
        String content="<a href='http://localhost/user/active?code="+user.getCode()+"'>点击激活【旅游网】</a>";
        MailUtils.sendMail(user.getEmail(),content,"激活邮件");

        return true;
    }


    /**
     * 激活用户
     *
     * @param code
     * @return
     */
    @Override
    public boolean active(String code) {

        User user = userDao.findByCode(code);
        if(user == null) {
            return false;
        }

        userDao.updateStatus(user.getUid());

        return true;

    }

    /**
     * 用户登录
     *
     * @param user
     * @return
     */
    @Override
    public User login(User user) {
        return userDao.findByUsernameAndPassword(user.getUsername(), user.getPassword());
    }
}

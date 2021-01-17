package com.pgman.goku.service.impl;

import com.pgman.goku.dao.IUserDao;
import com.pgman.goku.domain.Role;
import com.pgman.goku.domain.UserInfo;
import com.pgman.goku.service.IUserService;
import com.pgman.goku.util.BCryptPasswordEncoderUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

@Service("userService") // 这个名称要与 spring-security.xml文件中配置的登录验证方式 指定的service相同，同时覆写 loadUserByUsername()方法
@Transactional
public class UserServiceImpl implements IUserService {


    @Autowired
    private IUserDao userDao;

    @Autowired
    private BCryptPasswordEncoder bCryptPasswordEncoder;

    @Override
    public void addRoleToUser(String userId, String[] roleIds) {

        for (String roleId : roleIds) {
            userDao.addRoleToUser(userId, roleId);
        }
    }

    @Override
    public List<Role> findOtherRoles(String userId) {
        return userDao.findOtherRoles(userId);
    }

    @Override
    public UserInfo findById(String id) throws Exception {
        return userDao.findById(id);
    }

    @Override
    public void save(UserInfo userInfo) throws Exception {
        //对密码进行加密处理
//        userInfo.setPassword(bCryptPasswordEncoder.encode(userInfo.getPassword()));
        userInfo.setPassword(BCryptPasswordEncoderUtils.encodePassword(userInfo.getPassword()));
        userDao.save(userInfo);
    }

    @Override
    public List<UserInfo> findAll() throws Exception {
        return userDao.findAll();
    }

    /**
     * srping security数据库登录验证方法
     *
     * @param username
     * @return
     * @throws UsernameNotFoundException
     */
    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        UserInfo userInfo = null;
        try {
            userInfo = userDao.findByUsername(username);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 处理自己的用户对象封装成UserDetails
        //  User user=new User(userInfo.getUsername(),"{noop}"+userInfo.getPassword(),getAuthority(userInfo.getRoles()));
        User user = new User(
                userInfo.getUsername(),
//                "{noop}"+userInfo.getPassword(), // 未启用加密方式
                userInfo.getPassword(),
                userInfo.getStatus() == 0 ? false : true,
                true,
                true,
                true,
                getAuthority(userInfo.getRoles())
        );
        return user;
    }

    // 作用就是返回一个List集合，集合中装入的是角色描述
    public List<SimpleGrantedAuthority> getAuthority(List<Role> roles) {

        List<SimpleGrantedAuthority> list = new ArrayList<>();
        for (Role role : roles) {
            list.add(new SimpleGrantedAuthority("ROLE_" + role.getRoleName()));
        }
        return list;
    }

}

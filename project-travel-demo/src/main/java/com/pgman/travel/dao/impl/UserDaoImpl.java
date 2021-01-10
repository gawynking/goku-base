package com.pgman.travel.dao.impl;

import com.pgman.travel.dao.UserDao;
import com.pgman.travel.domain.User;
import com.pgman.travel.util.JDBCUtils;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

public class UserDaoImpl implements UserDao {

    private JdbcTemplate template = new JdbcTemplate(JDBCUtils.getDataSource());


    /**
     * 保存用户
     *
     * @param user
     */
    @Override
    public void save(User user) {

        String sql = "insert into tab_user(username,password,name,birthday,sex,telephone,email,status,code)values(?,?,?,?,?,?,?,?,?)";

        template.update(sql,
                user.getUsername(),
                user.getPassword(),
                user.getName(),
                user.getBirthday(),
                user.getSex(),
                user.getTelephone(),
                user.getEmail(),
                user.getStatus(),
                user.getCode());

    }

    /**
     * 根据用户名查询user信息
     * @param username
     * @return
     */
    @Override
    public User findByUsername(String username) {

        User user = null;
        String sql = "select * from tab_user where username = ?";
       try {
           user = template.queryForObject(sql, new BeanPropertyRowMapper<User>(User.class), username);
       }catch (Exception e){
           e.printStackTrace();
       }

        return user;
    }


    /**
     * 根据code查询用户
     *
     * @param code
     * @return
     */
    @Override
    public User findByCode(String code) {

        User user = null;
        String sql = "select * from tab_user where code = ?";
        try {
            user = template.queryForObject(sql, new BeanPropertyRowMapper<User>(User.class), code);
        }catch (Exception e){
            e.printStackTrace();
        }

        return user;
    }


    /**
     * 更新status
     *
     * @param uid
     * @return
     */
    @Override
    public int updateStatus(int uid) {

        String sql = "update tab_user set status = 'Y' where uid = ?";

        int cnt = template.update(sql, uid);

        return cnt;
    }


    /**
     * 根据用户名和密码查询用户
     *
     * @param username
     * @param password
     * @return
     */
    @Override
    public User findByUsernameAndPassword(String username, String password) {
        User user = null;
        String sql = "select * from tab_user where username = ? and password = ?";
        try {
            user = template.queryForObject(sql, new BeanPropertyRowMapper<User>(User.class), username,password);
        }catch (Exception e){
            e.printStackTrace();
        }

        return user;
    }

}

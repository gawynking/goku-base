package com.pgman.goku.mybatis.session.defaults;

import com.pgman.goku.mybatis.config.Configuration;
import com.pgman.goku.mybatis.session.SqlSession;
import com.pgman.goku.mybatis.session.SqlSessionFactory;

public class DefaultSqlSessionFactory implements SqlSessionFactory {

    private Configuration config;
    public DefaultSqlSessionFactory(Configuration config){
        this.config = config;
    }

    public SqlSession openSession() {

        return new DefaultSqlSession(config);

    }

}

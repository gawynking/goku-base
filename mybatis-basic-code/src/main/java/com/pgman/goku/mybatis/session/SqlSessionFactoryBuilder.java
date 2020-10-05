package com.pgman.goku.mybatis.session;

import com.pgman.goku.mybatis.config.Configuration;
import com.pgman.goku.mybatis.session.defaults.DefaultSqlSessionFactory;
import com.pgman.goku.mybatis.utils.XMLConfigBuilder;

import java.io.InputStream;

public class SqlSessionFactoryBuilder {

    /**
     * 实例化SqlSessionFactory
     * @param inputStream
     * @return
     */
    public SqlSessionFactory build(InputStream inputStream){
        Configuration config = XMLConfigBuilder.loadConfiguration(inputStream);
        return new DefaultSqlSessionFactory(config);
    }

}

package com.pgman.goku.mybatis.session.defaults;

import com.pgman.goku.mybatis.config.Configuration;
import com.pgman.goku.mybatis.session.SqlSession;
import com.pgman.goku.mybatis.session.proxy.MapperProxy;
import com.pgman.goku.mybatis.utils.DataSourceUtil;

import java.lang.reflect.Proxy;
import java.sql.Connection;

public class DefaultSqlSession implements SqlSession{

    private Configuration config;
    private Connection connection;

    public DefaultSqlSession(Configuration config){
        this.config = config;
        connection = DataSourceUtil.getConnection(config);
    }

    public <T> T getMapper(Class<T> daoInterfaceClass) {
        return (T) Proxy.newProxyInstance(daoInterfaceClass.getClassLoader(),
                new Class[]{daoInterfaceClass},
                new MapperProxy(config.getMappers(),connection));
    }

    public void close() {

        if(connection != null) {
            try{
                connection.close();
            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }

}

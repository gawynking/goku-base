package com.pgman.goku.config;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;

import javax.sql.DataSource;

/**
 * 和spring连接数据库相关的配置类
 */

public class JdbcConfig {

//     第一种：
//    /**
//     * 创建一个QueryRunner对象
//     * @param dataSource
//     * @return
//     */
//    @Bean(name = "runner")
//    @Scope("prototype")
//    public QueryRunner createQueryRunner(DataSource dataSource){
//        return new QueryRunner(dataSource);
//    }
//
//    /**
//     * 创建数据源对象
//     * @return
//     */
//    @Bean(name = "dataSource")
//    public DataSource createDataSource1(){
//        try {
//            ComboPooledDataSource dataSource = new ComboPooledDataSource();
//            dataSource.setDriverClass("com.mysql.jdbc.Driver");
//            dataSource.setJdbcUrl("jdbc:mysql://localhost:3306/mybatis");
//            dataSource.setUser("root");
//            dataSource.setPassword("mysql");
//            return dataSource;
//        }catch (Exception e){
//            throw new RuntimeException(e);
//        }
//    }

    // 第二种：
//
//    @Value("${jdbc.driver}")
//    private String driver;
//
//    @Value("${jdbc.url}")
//    private String url;
//
//    @Value("${jdbc.username}")
//    private String username;
//
//    @Value("${jdbc.password}")
//    private String password;
//
//    /**
//     * 创建一个QueryRunner对象
//     * @param dataSource
//     * @return
//     */
//    @Bean(name = "runner")
//    @Scope("prototype")
//    public QueryRunner createQueryRunner(DataSource dataSource){
//        return new QueryRunner(dataSource);
//    }
//
//    /**
//     * 创建数据源对象
//     * @return
//     */
//    @Bean(name = "dataSource")
//    public DataSource createDataSource1(){
//        try {
//            ComboPooledDataSource dataSource = new ComboPooledDataSource();
//            dataSource.setDriverClass(driver);
//            dataSource.setJdbcUrl(url);
//            dataSource.setUser(username);
//            dataSource.setPassword(password);
//            return dataSource;
//        }catch (Exception e){
//            throw new RuntimeException(e);
//        }
//    }


//    第三种：

    @Value("${jdbc.driver}")
    private String driver;

    @Value("${jdbc.url}")
    private String url;

    @Value("${jdbc.username}")
    private String username;

    @Value("${jdbc.password}")
    private String password;

    /**
     * 用于创建一个QueryRunner对象
     * @param dataSource
     * @return
     */
    @Bean(name="runner")
    @Scope("prototype")
    public QueryRunner createQueryRunner(@Qualifier("dataSource1") DataSource dataSource){
        return new QueryRunner(dataSource);
    }

    /**
     * 创建数据源对象
     * @return
     */
    @Bean(name="dataSource1")
    public DataSource createDataSource1(){
        try {
            ComboPooledDataSource dataSource = new ComboPooledDataSource();
            dataSource.setDriverClass(driver);
            dataSource.setJdbcUrl(url);
            dataSource.setUser(username);
            dataSource.setPassword(password);
            return dataSource;
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    @Bean(name="dataSource2")
    public DataSource createDataSource2(){
        try {
            ComboPooledDataSource dataSource = new ComboPooledDataSource();
            dataSource.setDriverClass(driver);
            dataSource.setJdbcUrl("jdbc:mysql://localhost:3306/test");
            dataSource.setUser(username);
            dataSource.setPassword(password);
            return dataSource;
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

}

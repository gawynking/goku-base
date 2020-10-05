package com.pgman.goku.c3p0;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import javax.sql.DataSource;
import java.sql.Connection;

public class ManualCodingExample {

    public static void main(String[] args) throws Exception{

        testDataSourceNumber();

    }


    public static void testDataSourceNumber() throws Exception{

        DataSource dataSource = getDataSource();

        for(int i = 1;i <= 11 ;i++){
            Connection connection = dataSource.getConnection();
            System.out.println("第 " + i + " 连接池对象 " + connection);

            // 这里的close不是指关闭连接 而是指将连接归还回池子
            connection.close();

        }

    }

    /**
     * 获取连接池对象
     *
     * @return
     * @throws Exception
     */
    public static DataSource getDataSource() throws Exception{

        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass("com.mysql.jdbc.Driver");
        cpds.setJdbcUrl("jdbc:mysql://localhost:3306/hera?serverTimezone=UTC");
        cpds.setUser("root");
        cpds.setPassword("mysql");

        cpds.setMaxPoolSize(10);
        cpds.setInitialPoolSize(5);
        cpds.setLoginTimeout(3);
        cpds.setMaxIdleTime(3);
        cpds.setCheckoutTimeout(1024);

        return cpds;

    }

}

package com.pgman.goku.druid;

import com.alibaba.druid.pool.DruidDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class ManualCodingExample {


    public static void main(String[] args) throws Exception{
        testDBCPConnection();
    }

    public static void testDBCPConnection() throws Exception{

        DataSource dataSource = getDataSource();

        Connection connection = dataSource.getConnection();

        PreparedStatement pstmt = connection.prepareStatement("select id,name from hera.hera_job");

        ResultSet resultSet = pstmt.executeQuery();

        while (resultSet.next()){
            System.out.println(resultSet.getInt("id") + ":" + resultSet.getString("name"));
        }


    }

    /**
     * 获取DBCP连接池
     *
     * @return
     */
    public static DataSource getDataSource() throws Exception{

        DruidDataSource dataSource = new DruidDataSource();

        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://localhost:3306/hera?serverTimezone=UTC");
        dataSource.setUsername("root");
        dataSource.setPassword("mysql");

        dataSource.setInitialSize(5);
        dataSource.setMaxActive(10);

        return dataSource;

    }

}

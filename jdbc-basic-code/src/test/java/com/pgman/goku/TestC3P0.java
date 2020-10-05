package com.pgman.goku;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.DataSources;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class TestC3P0 {


    public static void main(String[] args) throws Exception{

        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass("com.mysql.jdbc.Driver");
        cpds.setJdbcUrl("jdbc:mysql://localhost:3306/hera?serverTimezone=UTC");
        cpds.setUser("root");
        cpds.setPassword("mysql");

        cpds.setMaxPoolSize(10);
        cpds.setInitialPoolSize(5);

        Connection connection = cpds.getConnection();

        PreparedStatement preparedStatement = connection.prepareStatement("select id,name from hera.hera_job");

        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()){
            System.out.println(resultSet.getInt(1) + ":" + resultSet.getString(2));
        }

        connection.close();

        DataSources.destroy(cpds);

    }

}

package com.pgman.goku.c3p0;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import javax.sql.DataSource;
import java.sql.Connection;

public class ConfigCodingExample {

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

//        return new ComboPooledDataSource();
        return new ComboPooledDataSource("namedPool");

    }

}

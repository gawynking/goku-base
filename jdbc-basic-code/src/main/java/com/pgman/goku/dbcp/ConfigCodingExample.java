package com.pgman.goku.dbcp;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.util.Properties;

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
//            connection.close();

        }

    }


    /**
     * 获取连接池对象
     *
     * @return
     * @throws Exception
     */
    public static DataSource getDataSource() throws Exception{

        Properties properties = new Properties();

        InputStream in = ClassLoader.getSystemClassLoader().getResourceAsStream("dbcp.properties");

        properties.load(in);

        BasicDataSource dataSource = BasicDataSourceFactory.createDataSource(properties);

        return dataSource;

    }

}

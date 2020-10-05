package com.pgman.goku.test;

import com.pgman.goku.util.DateUtils;

import java.util.Date;

public class DateUtilsTest {
    public static void main(String[] args) throws Exception{

        System.out.println(new Date());

        System.out.println(DateUtils.dateFormat(new Date(),"yyyy-MM-dd HH:mm:ss"));




    }
}

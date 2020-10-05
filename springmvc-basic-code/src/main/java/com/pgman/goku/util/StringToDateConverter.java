package com.pgman.goku.util;

import org.springframework.core.convert.converter.Converter;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class StringToDateConverter implements Converter<String, Date> {

    @Override
    public Date convert(String s) {

        if(s == null){
            throw new RuntimeException("输入数据有误");
        }
        if("".equals(s)){
            throw new RuntimeException("输入数据有误");
        }

        DateFormat dataFormat = new SimpleDateFormat("yyyy-MM-dd");

        try {
            return dataFormat.parse(s);
        }catch (Exception e){
            e.printStackTrace();
        }

        return null;
    }
}

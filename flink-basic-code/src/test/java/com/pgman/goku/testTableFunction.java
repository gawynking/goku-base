package com.pgman.goku;

import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.util.GenerateTestDataUtils;

import java.util.List;

public class testTableFunction {

    public static void main(String[] args) {

        System.out.println(GenerateTestDataUtils.customers);

        List<JSONObject> customers = GenerateTestDataUtils.customers;

    }
}


/**
 {"birthday":"1995-03-15","address":"上海","create_time":"2020-01-24 01:01:22","sex":"女","name":"User-10","id":10}
 {"birthday":"1995-03-15","address":"上海","create_time":"2020-01-25 21:07:03","sex":"男","name":"User-11","id":11}
 {"birthday":"1995-03-15","address":"北京","create_time":"2020-01-27 15:15:39","sex":"男","name":"User-12","id":12}
 {"birthday":"2000-03-15","address":"上海","create_time":"2020-01-04 21:25:34","sex":"男","name":"User-13","id":13}
 {"birthday":"1995-03-15","address":"上海","create_time":"2020-01-24 19:54:43","sex":"男","name":"User-14","id":14}
 {"birthday":"1990-03-15","address":"北京","create_time":"2020-01-31 17:45:37","sex":"男","name":"User-15","id":15}
 */
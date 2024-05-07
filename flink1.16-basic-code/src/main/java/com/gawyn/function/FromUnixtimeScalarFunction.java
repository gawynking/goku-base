package com.gawyn.function;

import com.pgman.goku.util.DateUtils;
import org.apache.flink.table.functions.ScalarFunction;

public class FromUnixtimeScalarFunction extends ScalarFunction {


    /**
     * Flink UDF 定义
     *
     */
    public String eval(Long timestamp,String pattern) {

        System.out.println("Log => DateFormat " + System.currentTimeMillis());
       return DateUtils.fromUnixtime(timestamp,pattern);

    }

}

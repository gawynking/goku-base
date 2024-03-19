package com.gawyn.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;


@FunctionHint(output = @DataTypeHint("STRING"))
public class OrderTagTableFunction extends TableFunction {

    public void eval(String str) {
        for (String s : str.split(",")) {
            // use collect(...) to emit a row
            collect(s);
        }
    }

}

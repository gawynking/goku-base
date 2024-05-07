package com.gawyn.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;


@FunctionHint(output = @DataTypeHint("STRING"))
public class PassThroughFunction extends TableFunction {

    public void eval(String item) {
        collect(item);
    }

}

package com.pgman.goku.dp23.interpreter.calculator;

import java.util.HashMap;


/**
 * 变量解释器
 */
public class VarExpression extends Expression {

    private String key; // key=a,key=b,key=c

    public VarExpression(String key) {
        this.key = key;
    }

    // 根据变量的名称返回对应的值
    @Override
    public int interpreter(HashMap<String, Integer> var) {
        return var.get(this.key);
    }

}

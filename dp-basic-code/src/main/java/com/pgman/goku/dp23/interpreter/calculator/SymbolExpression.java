package com.pgman.goku.dp23.interpreter.calculator;

import java.util.HashMap;

/**
 * 抽象的运算符号解析器，这里每个运算符号都只和自己左右两个数字有关系
 * 但左右两个数字可能也是一个解析的结果
 */
public class SymbolExpression extends Expression {

    protected Expression left;
    protected Expression right;

    public SymbolExpression(Expression left, Expression right) {
        this.left = left;
        this.right = right;
    }

    // 默认实现，具体实现下推子类
    @Override
    public int interpreter(HashMap<String, Integer> var) {
        return 0;
    }

}

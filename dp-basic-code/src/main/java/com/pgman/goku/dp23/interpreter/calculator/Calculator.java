package com.pgman.goku.dp23.interpreter.calculator;

import java.util.HashMap;
import java.util.Stack;

/**
 * 计算器类
 */
public class Calculator {

    // 定义表达式
    private Expression expression;

    // 构造函数参数，并解析
    public Calculator(String expStr) { // expStr = a + b

        // 安排运算先后顺序
        Stack<Expression> stack = new Stack<>();

        // 表达式拆分成字符数组
        char[] charArray = expStr.toCharArray();


        Expression left = null;
        Expression right = null;

        // 遍历数组，根据不同的情况做处理
        for (int i = 0; i < charArray.length; i++) {
            switch (charArray[i]) {
                case '+': // 如果是+号，弹出左，新的右，最后加法表达式，结果压入栈
                    left = stack.pop();
                    right = new VarExpression(String.valueOf(charArray[++i]));
                    stack.push(new AddExpression(left, right));
                    break;
                case '-': // 如果是-号，弹出左，新的右，最后减法表达式，结果压入栈
                    left = stack.pop();
                    right = new VarExpression(String.valueOf(charArray[++i]));
                    stack.push(new SubExpression(left, right));
                    break;
                default:
                    // 如果是字符，将字符转化成字符串，并添加到stack里
                    stack.push(new VarExpression(String.valueOf(charArray[i])));
                    break;
            }
        }

        // 当遍历成整个数组后，stack就得到了最后的Expression
        this.expression = stack.pop();
    }

    public int run(HashMap<String, Integer> var) {
        // 最后将表达式a+b和var传递给Expression进行解释执行
        return this.expression.interpreter(var);
    }

}


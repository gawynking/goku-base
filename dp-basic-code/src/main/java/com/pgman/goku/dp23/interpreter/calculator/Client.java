package com.pgman.goku.dp23.interpreter.calculator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class Client {

    public static void main(String[] args) throws IOException {
        String expStr = getExpStr(); // 输入 a+b
        HashMap<String, Integer> var = getValue(expStr); // 解析成 - var {a=10, b=20}
        Calculator calculator = new Calculator(expStr);
        System.out.println("Result : " + expStr + " = " + calculator.run(var));
    }

    // 获取表达式
    public static String getExpStr() throws IOException {
        System.out.print("Input Expression : ");
        return (new BufferedReader(new InputStreamReader(System.in))).readLine();
    }

    //获取映射值
    public static HashMap<String, Integer> getValue(String expStr) throws IOException {
        HashMap<String, Integer> map = new HashMap<>();

        for (char ch : expStr.toCharArray()) {
            if (ch != '+' && ch != '-') {
                if (!map.containsKey(String.valueOf(ch))) {
                    System.out.print("Please Input " + String.valueOf(ch) + " value : ");
                    String in = (new BufferedReader(new InputStreamReader(System.in))).readLine();
                    map.put(String.valueOf(ch), Integer.valueOf(in));
                }
            }
        }

        return map;
    }

}

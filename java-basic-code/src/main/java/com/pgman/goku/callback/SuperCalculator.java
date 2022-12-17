package com.pgman.goku.callback;

public class SuperCalculator {
    public void add(int a, int b, Student stu) {
        int result = a + b;
        stu.fillBlank(a, b, result);
    }
}

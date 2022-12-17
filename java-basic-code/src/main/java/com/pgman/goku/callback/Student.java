package com.pgman.goku.callback;

/**
 * 参考链接: https://www.jianshu.com/p/56feb8ad1f56
 *
 * java回调机制依赖于接口和抽象类实现，通过向调用函数传递this参数实现被调用对象在处理结果后回调调用对象实现功能
 *
 * A调用B功能，B结束功能后调用A对象向A回传结果
 *
 */
public class Student {
    private String name;
    public Student(String name) {
        this.name = name;
    }
    public void callHelp(int a, int b) {
        new SuperCalculator().add(a, b, this);
    }
    public void fillBlank(int a, int b, int result) {
        System.out.println(name + "求助计算:" + a + "+" + b + "=" + result);
    }
}

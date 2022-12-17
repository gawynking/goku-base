package com.pgman.goku.callback;

/**
 * 执行流程：
 * 小黑通过自身的 callHelp() 调用了 new SuperCalculator() 的 add()，在调用的时候将自身的引用 this 当作参数一并传入，超级计算器在得出结果之后，回调了小黑的 fillBlank()，将结果填在了黑板的空格上。如此就体现了回调。此时，小黑的 fillBlank() 就是常说的回调函数。
 *
 * 通过这种方式，可以明显的看出，对于完成老师的填空题这个问题上，小黑已经不需要等待到加法做完且结果填写在黑板上才能去跟小伙伴撒欢了，填空这个工作由超级计算器小白来做了。回调的优势已经开始体现了。
 *
 */
public class CallbackDemo {

    public static void main(String[] args) {
        int a = 1357;
        int b = 2468;
        Student stu = new Student("小黑");
        stu.callHelp(a, b);
    }

}

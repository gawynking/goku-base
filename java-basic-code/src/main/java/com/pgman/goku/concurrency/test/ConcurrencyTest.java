package com.pgman.goku.concurrency.test;

/**
 * 并发编程一定快吗？
 * 可以用下面代码可以测试一下
 * 通过调整count次数，测试可以发现并发执行不一定速度快
 * 上下文切换次数会影响并发程序的性能
 */
public class ConcurrencyTest {

    private static final long count = 1000000000;

    public static void main(String[] args) throws InterruptedException {
        concurrency();
        serial();
    }

    private static void concurrency() throws InterruptedException {
        long start = System.currentTimeMillis();
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                int a = 0;
                for (long i = 0; i < count; i++) {
                    a += 5;
                }
            }
        });
        thread.start();

        int b = 0;
        for (long i = 0;i<count; i++){
            b--;
        }

        thread.join();
        long time = System.currentTimeMillis()-start;
        System.out.println("concurrency : " + time + "ms,b="+b);

    }

    private static void serial(){
        long start = System.currentTimeMillis();
        int a = 0;
        for (long i=0; i<count; i++){
            a += 5;
        }
        int b = 0;
        for (long i = 0;i<count; i++){
            b--;
        }
        long time = System.currentTimeMillis()-start;
        System.out.println("serial : " + time + "ms,b="+b+",a="+a);
    }

}

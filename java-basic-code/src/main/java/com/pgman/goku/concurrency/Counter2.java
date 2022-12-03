package com.pgman.goku.concurrency;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * volalite 声明变量针对 volatile++ 类型操作无法保证原子性
 */
public class Counter2 {

    private int i = 0; // 计数非线程安全最后值
    private volatile int j = 0;

    public static void main(String[] args) {

        final Counter2 cas = new Counter2();
        ArrayList<Thread> ts = new ArrayList<>(600);
        long start = System.currentTimeMillis();
        for (int j=0;j<100;j++){
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 10000; i++) {
                        cas.counti();
                        cas.countj();
                    }
                }
            });

            ts.add(t);
        }

        for(Thread t:ts){
            t.start();
        }

        // 等待线程执行完成
        for(Thread t:ts){
            try {
                t.join();
            }catch (InterruptedException e){
                e.printStackTrace();
            }

        }

        System.out.println(cas.i);
        System.out.println(cas.j);
        System.out.println(System.currentTimeMillis()-start);
    }

    private void countj(){
        j++;
    }
    /**
     * 非final
     */
    private void counti(){
        i++;
    }
}

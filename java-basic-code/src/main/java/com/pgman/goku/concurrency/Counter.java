package com.pgman.goku.concurrency;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class Counter {

    private AtomicInteger atomicI = new AtomicInteger(0); // 记录线程安全最后值
    private int i = 0; // 计数非线程安全最后值

    public static void main(String[] args) {

        final Counter cas = new Counter();
        ArrayList<Thread> ts = new ArrayList<>(600);
        long start = System.currentTimeMillis();
        for (int j=0;j<100;j++){
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 10000; i++) {
                        cas.count();
                        cas.safeCount();
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
        System.out.println(cas.atomicI.get());
        System.out.println(System.currentTimeMillis()-start);
    }

    /**
     * 线程安全计数
     */
    private void safeCount(){
        for (;;){
            int i = atomicI.get();
            boolean suc = atomicI.compareAndSet(i, ++i);
            if(suc){
                break;
            }

        }
    }

    /**
     * 非线程安全计数
     */
    private void count(){
        i++;
    }
}

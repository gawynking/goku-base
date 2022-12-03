package com.pgman.goku.concurrency.lock;

import scala.collection.parallel.immutable.ParRange;
import sun.awt.windows.ThemeReader;

/**
 * 这是一个多线程死锁示例
 *
 * 避免死锁建议：
 * - 避免一个线程同时获取多个锁资源；
 * - 避免一个线程在锁内同时占用多个资源，尽量保证每个锁只占用一个资源；
 * - 尝试使用定时锁，使用Lock.tryLock(timeout)来替代使用内部锁机制；
 */
public class DeadLockDemo {

    private static String A = "A";
    private static String B = "B";

    public static void main(String[] args) {
        new DeadLockDemo().deadLock();
    }

    private void deadLock(){
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                synchronized (A){
                    try {
                        Thread.currentThread().sleep(2000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    synchronized (B){
                        System.out.println(1);
                    }
                }
            }
        });

        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                synchronized (B) {
                    synchronized (A) {
                        System.out.println(2);
                    }
                }
            }
        });

        t1.start();
        t2.start();

    }

}

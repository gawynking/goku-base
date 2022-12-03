package com.pgman.goku.concurrency.morethread;

import jdk.jfr.events.TLSHandshakeEvent;

import javax.management.ObjectName;
import java.util.concurrent.TimeUnit;

/**
 * 安全停止线程两种方式：
 * - 通过interrupt()通知线程中止
 * - 通过boolean变量方式中止线程
 *
 * 通过stop()方式停止线程不安全不建议使用
 */
public class SafeStopThread {

    public static void main(String[] args) throws InterruptedException {
        Runner one = new Runner();
        Thread countThread = new Thread(one, "CountThread");
        countThread.start();
        TimeUnit.SECONDS.sleep(1);
        countThread.interrupt(); // 1-通过interrupt()调用停止线程

        Runner two = new Runner();
        countThread = new Thread(two, "CountThread");
        countThread.start();
        TimeUnit.SECONDS.sleep(1);
        two.cancel();// 2-通过boolean变量方式停止线程

        Runner three = new Runner();
        countThread = new Thread(three, "CountThread");
        countThread.start();
        TimeUnit.SECONDS.sleep(1);
        countThread.stop(); // 3-通过调用stop()方式停止线程，不推荐使用

    }


    static class Runner implements Runnable{

        private long i;
        private volatile boolean on = true;

        @Override
        public void run() {
            while (on && !Thread.currentThread().isInterrupted()){
                i++;
            }
            System.out.println("Count i = " + i);
        }

        public void cancel(){
            on=false;
        }
    }
}

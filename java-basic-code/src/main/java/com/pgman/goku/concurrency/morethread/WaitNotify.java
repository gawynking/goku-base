package com.pgman.goku.concurrency.morethread;

import com.pgman.goku.util.SleepUtils;
import scala.collection.parallel.mutable.ParArray;
import sun.awt.windows.ThemeReader;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * 等待/通知机制是指一个线程a调用了对象o的wait()方法进入等待状态，而另一个线程b调用了对象o的notify()方法，线程a收到通知后从对象o的wait()方法返回，
 * 进行执行后续操作；
 * 上述两个线程通过贡献对象o进行通信交互。
 *
 * 等待/通知机制可以很好的组织多个线程共同完成一个事情。
 *
 * 理解：
 * 1-使用wait()，notify()和notifyALL()是需要先对调用对象加锁；
 * 2-调用wait()方法后，线程状态由RUNNING变成WAITING，并将当前线程放置到对象等待队列；
 * 3-notify()或notifyALL()方法调用后，等待线程依旧不会从wait()返回，需要调用notify()或notifyALL()的线程释放锁后，等待线程才有机会从wait()返回
 * 4-notify()将等待队列中的一个等待线程从等待队列移动到同步队列中，而notifyAll()方法则是将等待队列中的所有线程移动到同步队列中，被移动的线程状态从WAITING编程BLOCKED。
 * 5-从wait()方法返回的前提是获得了调用兑现的锁。
 *
 * 等待/通知机制涉及到的两个基本数据结果：
 * - 等待队列
 * - 同步队列
 */
public class WaitNotify {
    static boolean flag = true;
    static Object lock = new Object();

    public static void main(String[] args) throws InterruptedException {
        Thread waitThread = new Thread(new Wait(), "WaitThread");
        waitThread.start();
        TimeUnit.SECONDS.sleep(1);
        Thread notifyThread = new Thread(new Notify(), "NotifyThread");
        notifyThread.start();

    }

    static class Wait implements Runnable{

        @Override
        public void run() {
            // 加锁，拥有lock的monitor
            synchronized (lock){
                // 当天件不满足时，继续等待,同时释放lock锁
                while (flag){
                    try{
                        System.out.println(Thread.currentThread() + "flag is true.wait @" + new SimpleDateFormat("HH:mm:ss").format(new Date()));
                        lock.wait();
                    }catch (Exception e){

                    }
                }
                // 条件满足时，完成工作
                System.out.println(Thread.currentThread() + "flag is false.running @" + new SimpleDateFormat("HH:mm:ss").format(new Date()));
            }
        }
    }


    static class Notify implements Runnable{

        @Override
        public void run() {
            // 加锁，拥有lock的monitor
            synchronized (lock){
                // 获取lock锁，然后通知，通知时不会释放lock锁
                // 知道当前线程释放lock锁后，waitthread才能从wait方法中返回
                while (flag){
                    System.out.println(Thread.currentThread() + " hold lock.notify @" + new SimpleDateFormat("HH:mm:ss").format(new Date()));
                    lock.notifyAll();
                    flag=false;
                    SleepUtils.second(3);
                }
                // 再次加锁
                synchronized (lock){
                    System.out.println(Thread.currentThread() + " hold lock again.sleep @" + new SimpleDateFormat("HH:mm:ss").format(new Date()));
                    SleepUtils.second(5);
                }
            }
        }
    }

}

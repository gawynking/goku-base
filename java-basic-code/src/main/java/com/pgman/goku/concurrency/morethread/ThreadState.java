package com.pgman.goku.concurrency.morethread;

import com.pgman.goku.util.SleepUtils;

public class ThreadState {

    public static void main(String[] args) {
        new Thread(new TimeWaiting(),"TimeWaitingThread()").start();
        new Thread(new Waiting(),"WaitingThread()").start();
        new Thread(new Blocked(),"BlockedThread-1").start();
        new Thread(new Blocked(),"BlockedThread-2").start();
    }

    // 该线程不断进行休眠
    static class TimeWaiting implements Runnable{

        @Override
        public void run() {
            while (true){
                SleepUtils.second(100);
            }
        }
    }

    // 该线程在Waiting.class实例上等待
    static class Waiting implements Runnable{

        @Override
        public void run() {
            while (true){
                synchronized (Waiting.class){
                    try {
                        Waiting.class.wait();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    // 该线程在Blocked.class 实例加载后，不会释放锁
    static class Blocked implements Runnable{

        @Override
        public void run() {
            synchronized (Blocked.class){
                while (true){
                    SleepUtils.second(100);
                }
            }
        }
    }
}

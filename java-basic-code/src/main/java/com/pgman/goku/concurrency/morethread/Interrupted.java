package com.pgman.goku.concurrency.morethread;

import com.pgman.goku.util.SleepUtils;

import java.util.concurrent.TimeUnit;

/**
 * 声明跑出InterruptedException异常的方法在跑出InterruptedException之前会先将isInterrupt()复位，此时调用isInterrupt()会依然是false
 */
public class Interrupted {


    public static void main(String[] args) throws InterruptedException {

        Thread sleepThread = new Thread(new SleepRunner(), "SleepRunnerThread");
        sleepThread.setDaemon(true);

        Thread busyThread = new Thread(new BusyRunner(), "BusyRunner");
        busyThread.setDaemon(true);

        sleepThread.start();
        busyThread.start();

        TimeUnit.SECONDS.sleep(5);
        sleepThread.interrupt();
        busyThread.interrupt();

        System.out.println("sleepThread interrupted is " + sleepThread.isInterrupted());
        System.out.println("busyThread interrupted is " + busyThread.isInterrupted());

        SleepUtils.second(2);

    }


    static class SleepRunner implements Runnable{

        @Override
        public void run() {
            while (true){
                SleepUtils.second(10);
            }
        }
    }

    static class BusyRunner implements Runnable{

        @Override
        public void run() {
            while (true){
            }
        }
    }

}

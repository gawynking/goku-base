package com.pgman.goku.concurrency.morethread;

import com.pgman.goku.util.SleepUtils;
import sun.awt.windows.ThemeReader;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * 过期api，不建议使用
 */
public class ThreadDeprecated {

    public static void main(String[] args) throws InterruptedException {
        DateFormat format = new SimpleDateFormat("HH:mm:ss");
        Thread printThread = new Thread(new Runner(), "PrintThread");
        printThread.setDaemon(true);
        printThread.start();
        TimeUnit.SECONDS.sleep(3);
        printThread.suspend();
        System.out.println("main suspend printThread as " + format.format(new Date()));
        TimeUnit.SECONDS.sleep(3);
        printThread.resume();
        System.out.println("main resume printThread as " + format.format(new Date()));
        TimeUnit.SECONDS.sleep(3);
        printThread.stop();
        System.out.println("main stop printThread as " + format.format(new Date()));
        TimeUnit.SECONDS.sleep(3);
    }

    static class Runner implements Runnable{

        DateFormat format = new SimpleDateFormat("HH:mm:ss");

        @Override
        public void run() {
            while (true){
                System.out.println(Thread.currentThread().getName()+" Run at :" +format.format(new Date()));
                SleepUtils.second(1);
            }
        }
    }

}

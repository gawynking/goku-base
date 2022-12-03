package com.pgman.goku.concurrency.morethread;

import com.pgman.goku.util.SleepUtils;

public class Deamon {

    public static void main(String[] args) {
        Thread thread = new Thread(new DeamonRunner(), "DeamonRunner");
        thread.setDaemon(true);
        thread.start();
    }


    static class DeamonRunner implements Runnable{

        @Override
        public void run() {
            try {
                SleepUtils.second(10);
            }catch (Exception e){
                System.out.println("DeamonThread finally run.");
            }
        }
    }
}

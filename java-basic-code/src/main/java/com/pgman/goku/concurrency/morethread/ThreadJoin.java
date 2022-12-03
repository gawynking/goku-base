package com.pgman.goku.concurrency.morethread;

import java.util.concurrent.TimeUnit;

/**
 * 调用了thread.join()方法的线程会等待join线程执行完成后返回
 */
public class ThreadJoin {

    public static void main(String[] args) throws InterruptedException {
        Thread previous = Thread.currentThread();
        for (int i=0;i<10;i++){
            // 每个线程拥有前一个线程的引用，需要等待前一个线程中止，才能从等待中返回
            Thread thread = new Thread(new Domino(previous), String.valueOf(i));
            thread.start();
            previous=thread;
        }
        TimeUnit.SECONDS.sleep(5);
        System.out.println(Thread.currentThread().getName()+"terminate.");
    }

    static class Domino implements Runnable{
        private Thread thread;
        public Domino(Thread thread){
            this.thread=thread;
        }

        @Override
        public void run() {
            try{
                thread.join();
            }catch (Exception e){
                e.printStackTrace();
            }
            System.out.println(Thread.currentThread().getName()+"terminate.");
        }
    }
}

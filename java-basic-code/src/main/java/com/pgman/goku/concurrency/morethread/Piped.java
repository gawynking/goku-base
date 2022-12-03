package com.pgman.goku.concurrency.morethread;

import java.io.IOException;
import java.io.PipedReader;
import java.io.PipedWriter;

/**
 * 管道流
 */
public class Piped {

    public static void main(String[] args) throws IOException {
        PipedWriter out = new PipedWriter();
        PipedReader in = new PipedReader();
        out.connect(in);

        Thread pringReader = new Thread(new Print(in), "PringReader");
        pringReader.start();

        int receive = 0;
        try{
            while ((receive=System.in.read())!=-1){
                out.write(receive);
            }
        }catch (Exception e){
        }finally {
            out.close();
        }
    }


    static class Print implements Runnable{
        private PipedReader in;
        public Print(PipedReader in){
            this.in = in;
        }

        @Override
        public void run() {
            int receive = 0;
            try{
                while ((receive=in.read())!=-1){
                    System.out.print((char)receive);
                }
            }catch (Exception e){
            }
        }
    }
}

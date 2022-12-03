package com.pgman.goku.util;

import java.util.concurrent.TimeUnit;

public class SleepUtils {
    public static final void second(long secouds){
        try{
            TimeUnit.SECONDS.sleep(secouds);
        }catch (InterruptedException e){
            e.printStackTrace();
        }
    }
}

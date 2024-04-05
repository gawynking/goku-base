package com.pgman.goku.test;

import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

public class RedissonTest {

    @Test
    public void testDistrube(){

        Redisson redisson = redissonCreate();
        RLock lock = redisson.getLock("a");
        lock.lock();


        lock.unlock();

    }


    public Redisson redissonCreate(){
        Config config = new Config();
        config.useSingleServer().setAddress("redis://localhost:6379").setDatabase(0);
        config.setLockWatchdogTimeout(30000);
        Redisson redisson = (Redisson) Redisson.create(config);
        return redisson;
    }

}

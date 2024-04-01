package com.pgman.goku.redis;

import com.pgman.goku.redis.tool.JedisUtil;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RedisCacheDemo {

    JedisUtil jedisUtil = JedisUtil.getInstance();

    @Test
    public void test(){

        for(int i =0 ; i<10000;i++){
            jedisUtil.STRINGS.set("user",String.valueOf(i));
        }

        String user = jedisUtil.STRINGS.get("user");
        System.out.println(user);

    }

    @Test
    public void test01(){
        jedisUtil.KEYS.flushAll();

        Map map1 = new HashMap<String,String>();
        map1.put("user_name","gawynking");
        map1.put("birthday","1990-03-15");
        map1.put("job","总统");

        jedisUtil.HASHS.hmset("a1",map1);

        Map map2 = new HashMap<String,String>();
        map2.put("user_name","gawynking1");
        map2.put("birthday","1990-03-16");
        map2.put("job","主席");

        jedisUtil.HASHS.hmset("a2",map2);

        Map<String, String> stringStringMap = jedisUtil.HASHS.hgetAll("a1");
        System.out.println(stringStringMap);

        String job = jedisUtil.HASHS.hget("1", "job");
        System.out.println(job);



        Set<String> as = jedisUtil.KEYS.keys("a*");
        for(String a: as){
            System.out.println("---------- : " + a);
        }

        jedisUtil.KEYS.flushKeys("a*");

        as = jedisUtil.KEYS.keys("a*");
        for(String a: as){
            System.out.println("---------- : " + a);
        }

    }



    @Test
    public void test02(){

        jedisUtil.SETS.sadd("set:1","a");
        jedisUtil.SETS.sadd("set:1","b");
        jedisUtil.SETS.sadd("set:1","c");
        jedisUtil.SETS.sadd("set:1","d");
        jedisUtil.SETS.sadd("set:1","d");
        jedisUtil.SETS.sadd("set:1","e");

        Set<String> set1 = jedisUtil.SETS.smembers("set:1");
        System.out.println(set1);

        long scard = jedisUtil.SETS.scard("set:1");
        System.out.println(scard);

    }

    @Test
    public void test03(){
        jedisUtil.HYPERLOGLOG.pfadd("hll1","a","a","b","c","d","c");
        jedisUtil.HYPERLOGLOG.pfadd("hll1","a1","a1","b1","c1","d1","c1");
        Long hll1 = jedisUtil.HYPERLOGLOG.pfcount("hll1");
        System.out.println(hll1);
    }

}

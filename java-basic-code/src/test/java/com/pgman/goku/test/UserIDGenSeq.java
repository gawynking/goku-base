package com.pgman.goku.test;

import com.pgman.goku.redis.tool.JedisUtil;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Set;

public class UserIDGenSeq {


    public static void main(String[] args) {

        JedisUtil jedisUtil = JedisUtil.getInstance();
        // 清除所有key值
        jedisUtil.KEYS.flushAll();

        // 将序列加入有序集合
        jedisUtil.TOOL.generateUniqueSequence("sortset:gawyn:01","100");
        jedisUtil.TOOL.generateUniqueSequence("sortset:gawyn:01","2");
        jedisUtil.TOOL.generateUniqueSequence("sortset:gawyn:01","55");
        jedisUtil.TOOL.generateUniqueSequence("sortset:gawyn:01","333");
        jedisUtil.TOOL.generateUniqueSequence("sortset:gawyn:01","100");
        jedisUtil.TOOL.generateUniqueSequence("sortset:gawyn:01","333");
        jedisUtil.TOOL.generateUniqueSequence("sortset:gawyn:01","555");
        jedisUtil.TOOL.generateUniqueSequence("sortset:gawyn:01","999");
        jedisUtil.TOOL.generateUniqueSequence("sortset:gawyn:01","666");


        // 遍历有序集合
        List<Tuple> tuples = jedisUtil.SORTSETS.zscan("sortset:gawyn:01");
        int i = 0;
        for(Tuple tuple:tuples){
            i++;
            String element = tuple.getElement();
            double score = tuple.getScore();
            System.out.println("Seq " + i + " --> " +element+" --> "+score);
        }

    }

}

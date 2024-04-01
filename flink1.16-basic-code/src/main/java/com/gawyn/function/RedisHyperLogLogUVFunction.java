package com.gawyn.function;

import com.pgman.goku.redis.tool.JedisUtil;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;


public class RedisHyperLogLogUVFunction extends AggregateFunction<Long, RedisHyperLogLogUVFunction.MyAcc> {

    private String prefix;

    private JedisUtil jedis;

    public RedisHyperLogLogUVFunction(){
        prefix = "uv:";
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        jedis = JedisUtil.getInstance();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public MyAcc createAccumulator() {
        return new MyAcc();
    }

    /**
     *
     * @param acc
     * @param groupKey : redis key，建议由任务名:分组键:分组键值组成
     * @param uvField : Set集合元素
     */
    public void accumulate(MyAcc acc, String groupKey, String uvField) {
        String redisKey = new String(new StringBuffer(prefix).append(groupKey));
        jedis.HYPERLOGLOG.pfadd(redisKey,uvField);
        acc.groupKey = redisKey;
        acc.uv = jedis.HYPERLOGLOG.pfcount(redisKey);
    }

    // HLL 不支持回撤
    public void retract(MyAcc acc, String groupKey, String uvField) {}
    public void merge(MyAcc acc, Iterable<MyAcc> it) {}

    public void resetAccumulator(MyAcc acc) {
        jedis.KEYS.del(acc.groupKey);
        acc.uv = 0L;
    }

    @Override
    public Long getValue(MyAcc myAcc) {
        return myAcc.uv;
    }

    public static class MyAcc {
        public String groupKey = null;
        public Long uv = 0L;
    }

}

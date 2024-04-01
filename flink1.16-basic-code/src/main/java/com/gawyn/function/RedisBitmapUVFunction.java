package com.gawyn.function;

import com.pgman.goku.redis.tool.JedisUtil;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;


public class RedisBitmapUVFunction extends AggregateFunction<Long, RedisBitmapUVFunction.MyAcc> {

    private String prefix;

    private JedisUtil jedis;

    public RedisBitmapUVFunction(){
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
     * @param offset : 值对应位移
     */
    public void accumulate(MyAcc acc, String groupKey, Long offset) {
        String redisKey = new String(new StringBuffer(prefix).append(groupKey));
        jedis.BITMAP.setBit(redisKey,offset,true);
        acc.groupKey = redisKey;
        acc.uv = jedis.BITMAP.bitCount(redisKey);
    }

    public void retract(MyAcc acc, String groupKey, Long offset) {
        String redisKey = new String(new StringBuffer(prefix).append(groupKey));
        jedis.BITMAP.setBit(redisKey,offset,false);
        acc.uv = jedis.BITMAP.bitCount(redisKey);
    }

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

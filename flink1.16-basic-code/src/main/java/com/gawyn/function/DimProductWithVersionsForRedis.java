package com.gawyn.function;

import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.redis.tool.JedisUtil;
import com.pgman.goku.util.JSONUtils;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import java.util.Map;
import java.util.Set;

public class DimProductWithVersionsForRedis extends ScalarFunction {

    private static JedisUtil jedis;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        jedis = JedisUtil.getInstance();
    }

    /**
     * Flink 维表查询 UDF 定义
     *
     * @param nkey
     * @param timestamp
     */
    public String eval(String nkey,Long timestamp) {
        // 1 构造Redis Sorted set集合key
        String key = "dim_data:"+nkey;

        // 2 从Sorted set集合获取key对应的版本数据集合
        Set<String> values = jedis.SORTSETS.zrange(key,0,-1);

        // 3 获取对应的版本数据
        String versionKey = null;
        for(String value:values){
            String[] split = value.split(":");
            if(Long.valueOf(split[2]) > timestamp){
                break;
            }else {
                versionKey = value;
            }
        }

        // 4 构造hash对应的key
        String hashKey = null;
        if(versionKey != null){
            hashKey = "versions:"+versionKey;
        }

        // 5 获取版本数据
        Map<String, String> versionValue = jedis.HASHS.hgetAll(hashKey);

        // 6 返回数据
        String result = JSONUtils.getJSONObjectFromMap(versionValue).toJSONString();
        System.out.println("Log => " + System.currentTimeMillis() + " : " + result);
        return result;
    }


    /**
     * 接收维度数据流，同步更新Redis
     *
     * @param nkey
     * @param timestamp
     * @param dim
     */
    public static void updataRedis(String nkey,long timestamp,String dim){

        JSONObject jsonDim = JSONObject.parseObject(dim);

        // 1 构造Redis Sorted set集合key
        String zKey = "dim_data:"+nkey;

        // 2 构造Redis Sorted set集合value
        String zValue = nkey+":"+timestamp;

        // 3 更新Redis Sorted set集合
        jedis.SORTSETS.zadd(zKey,timestamp,zValue);

        // 4 构造Hase结构key
        String hKey = "versions:"+zValue;

        // 5 构造Hase结构value
        Map hValue = JSONUtils.getMapFromJSONObject(jsonDim);

        // 6 更新Hase集合
        jedis.HASHS.hmset(hKey,hValue);

    }





    public static void testEval(String nkey,long timestamp) {
        // 1 构造Redis Sorted set集合key
        String key = "dim_data:"+nkey;

        // 2 从Sorted set集合获取key对应的版本数据集合
        Set<String> values = jedis.SORTSETS.zrange(key,0,-1);

        System.out.println(values);

        // 3 获取对应的版本数据
        String versionKey = null;
        for(String value:values){
            String[] split = value.split(":");
            if(Long.valueOf(split[2]) > timestamp){
                break;
            }else {
                versionKey = value;
            }
        }

        // 4 构造hash对应的key
        String hashKey = null;
        if(versionKey != null){
            hashKey = "versions:"+versionKey;
        }

        // 5 获取版本数据
        Map<String, String> versionValue = jedis.HASHS.hgetAll(hashKey);

        // 6 返回数据
        String result = JSONUtils.getJSONObjectFromMap(versionValue).toJSONString();
        System.out.println(System.currentTimeMillis() + " : " + result);

    }


    /**
     * 准备测试数据
     *
     * @param args
     */
    public static void main(String[] args) {

//        jedis.KEYS.flushAll();

//        String json = "{\"category_name\":\"Java\",\"category_id\":\"1\",\"publisher_date\":\"2024-05-05\",\"create_time\":\"2024-05-05 14:51:53\",\"update_time\":\"2024-05-05 14:51:53\",\"price\":\"2.03\",\"author\":\"Java-author:1393\",\"publisher\":\"Java-publisher:7202\",\"book_id\":\"2\",\"book_name\":\"Java-8727-版\"}";
//        String json = "{\"category_name\":\"Java\",\"category_id\":\"1\",\"publisher_date\":\"2024-05-05\",\"create_time\":\"2024-05-05 14:51:53\",\"update_time\":\"2024-05-05 15:51:53\",\"price\":\"12.03\",\"author\":\"Java-author:1393\",\"publisher\":\"Java-publisher:7202\",\"book_id\":\"2\",\"book_name\":\"Java-8727-版\"}";
//
//        String json = "{\"category_name\":\"SQL\",\"category_id\":\"2\",\"publisher_date\":\"2024-05-05\",\"create_time\":\"2024-05-05 14:51:57\",\"update_time\":\"2024-05-05 14:51:57\",\"price\":\"0.91\",\"author\":\"SQL-author:9969\",\"publisher\":\"SQL-publisher:2286\",\"book_id\":\"6\",\"book_name\":\"SQL-4819-版\"}\n";
//        String json = "{\"category_name\":\"SQL\",\"category_id\":\"2\",\"publisher_date\":\"2024-05-05\",\"create_time\":\"2024-05-05 14:51:57\",\"update_time\":\"2024-05-05 15:51:57\",\"price\":\"10.91\",\"author\":\"SQL-author:9969\",\"publisher\":\"SQL-publisher:2286\",\"book_id\":\"6\",\"book_name\":\"SQL-4819-版\"}\n";


//        JSONObject value = JSONObject.parseObject(json);
//
//
//        String key = "dim_book:"+value.getString("book_id");
//        Long timestmap = DateUtils.getTimestamp(value.getString("update_time"),"yyyy-MM-dd HH:mm:ss");
//
//        System.out.println(key);
//        System.out.println(timestmap);
//        System.out.println(value);
//
//
//        updataRedis(key,timestmap,value.toJSONString());


        String key = "dim_book:6";
        Long timestamp = 1814891913000l;

        testEval(key,timestamp);

    }

}

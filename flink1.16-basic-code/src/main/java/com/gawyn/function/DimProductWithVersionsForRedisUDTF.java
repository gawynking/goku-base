package com.gawyn.function;

import com.alibaba.fastjson.JSONObject;
import com.pgman.goku.redis.tool.JedisUtil;
import com.pgman.goku.util.JSONUtils;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import scala.reflect.internal.util.TableDef;

import java.util.Map;
import java.util.Set;

@FunctionHint(output = @DataTypeHint("STRING"))
public class DimProductWithVersionsForRedisUDTF extends TableFunction {

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
    public void eval(String nkey,Long timestamp) {
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
        collect(result);
    }


}

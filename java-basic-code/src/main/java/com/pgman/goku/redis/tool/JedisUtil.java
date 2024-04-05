package com.pgman.goku.redis.tool;

import com.pgman.goku.redis.config.JRedisPoolConfig;
import org.apache.log4j.Logger;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.config.Config;
import redis.clients.jedis.*;
import redis.clients.jedis.util.SafeEncoder;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;


public class JedisUtil {

    private Logger log = Logger.getLogger(JedisUtil.class);

    /**
     * 缓存生存时间
     */
    private final int expire = 60000;

    /**
     * 操作Key的方法
     */
    public Keys KEYS = new Keys();

    /**
     * 对存储结构为String类型的操作
     */
    public Strings STRINGS = new Strings();

    /**
     * 对存储结构为List类型的操作
     */
    public Lists LISTS = new Lists();

    /**
     * 对存储结构为Set类型的操作
     */
    public Sets SETS = new Sets();

    /**
     * 对存储结构为HashMap类型的操作
     */
    public Hashs HASHS = new Hashs();

    /**
     * 对存储结构为Set(排序的)类型的操作
     */
    public SortSets SORTSETS = new SortSets();


    public HyperLogLog HYPERLOGLOG = new HyperLogLog();

    public Bitmap BITMAP = new Bitmap();

    public Tool TOOL = new Tool();

    private static JedisPool jedisPool = null;

    static {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(JRedisPoolConfig.MAX_IDLE);
        config.setMaxWaitMillis(JRedisPoolConfig.MAX_WAIT);

        //redis如果设置了密码：
//        jedisPool = new JedisPool(
//                        config,
//                        JRedisPoolConfig.REDIS_IP,
//                        JRedisPoolConfig.REDIS_PORT,
//                        10000,
//                        JRedisPoolConfig.REDIS_PASSWORD
//                    );

        //redis未设置了密码：
        jedisPool = new JedisPool(
                config,
                JRedisPoolConfig.HOSTNAME,
                JRedisPoolConfig.PORT
        );

    }

    public static Redisson getRedisson(){
        Config config = new Config();
        config.useSingleServer().setAddress("redis://localhost:6379").setDatabase(0);
//        config.setLockWatchdogTimeout(30000);
        return (Redisson) Redisson.create(config);
    }


    public JedisPool getPool() {
        return jedisPool;
    }

    /**
     * 从jedis连接池中获取获取jedis对象
     */
    public Jedis getJedis() {
        return jedisPool.getResource();
    }

    public Jedis getJedis(int db) {
        Jedis jedis = getJedis();
        jedis.select(db);
        return jedis;
    }

    private static final JedisUtil jedisUtil = new JedisUtil();


    /**
     * 获取JedisUtil实例
     */
    public static JedisUtil getInstance() {
        return jedisUtil;
    }

    /**
     * 回收jedis
     */
    public void returnJedis(Jedis jedis) {
        jedisPool.returnResource(jedis);
    }


    //******************************************* other *******************************************//
    public class Tool{

        /**
         * 为离散值生成序列化的ID序列
         *
         * @param key
         * @param value
         * @return
         */
        public Long generateUniqueSequence(String key,String value){
            Redisson redisson = getRedisson();
            RLock lock = redisson.getLock("Lock:"+key);
            Long rank = -1L;
            try {
                rank = SORTSETS.zrank(key, value);
                if (rank != -1) {
                    return rank;
                } else {
                    boolean isLocked = lock.tryLock(10, 10, java.util.concurrent.TimeUnit.SECONDS);
                    if(isLocked){
                        rank = SORTSETS.zcard(key);
                        SORTSETS.zadd(key, rank, value);
                    }
                }
            }catch (InterruptedException e){}finally {
                lock.unlock();
            }
            return rank;
        }



    }

    //*******************************************Keys*******************************************//
    public class Keys {

        /**
         * 删除keys对应的记录,可以是多个key
         */
        public long del(String... keys) {
            Jedis jedis = getJedis();
            long count = jedis.del(keys);
            returnJedis(jedis);
            return count;
        }

        /**
         * 删除keys对应的记录,可以是多个key
         */
        public long del(byte[]... keys) {
            Jedis jedis = getJedis();
            long count = jedis.del(keys);
            returnJedis(jedis);
            return count;
        }

        /**
         * 序列化给定 key ，并返回被序列化的值。
         * @param key
         * @return
         */
        public byte[] dump(String key){
            Jedis jedis = getJedis();
            byte[] dump = jedis.dump(key);
            returnJedis(jedis);
            return dump;
        }

        public byte[] dump(byte[] key){
            Jedis jedis = getJedis();
            byte[] dump = jedis.dump(key);
            returnJedis(jedis);
            return dump;
        }

        /**
         * 判断key是否存在
         */
        public boolean exists(String key) {
            Jedis jedis = getJedis();
            boolean exists = jedis.exists(key);
            returnJedis(jedis);
            return exists;
        }

        public Long exists(String... keys) {
            Jedis jedis = getJedis();
            Long exists = jedis.exists(keys);
            returnJedis(jedis);
            return exists;
        }

        public Long exists(byte[]... keys) {
            Jedis jedis = getJedis();
            Long exists = jedis.exists(keys);
            returnJedis(jedis);
            return exists;
        }

        /**
         * 设置过期时间
         */
        public long expire(String key, int seconds) {
            if (seconds <= 0) {
                return -1;
            }
            Jedis jedis = getJedis();
            long count = jedis.expire(key, seconds);
            returnJedis(jedis);
            return count;
        }

        /**
         * 设置默认过期时间
         */
        public void expire(String key) {
            expire(key, expire);
        }


        /**
         * 清楚所有数据
         */
        public String flushAll() {
            Jedis jedis = getJedis();
            String stata = jedis.flushAll();
            returnJedis(jedis);
            return stata;
        }


        /**
         * 重命名key
         */
        public String rename(String oldkey, String newkey) {
            return rename(SafeEncoder.encode(oldkey), SafeEncoder.encode(newkey));
        }

        /**
         * 更改key,仅当新key不存在时才执行
         */
        public long renamenx(String oldkey, String newkey) {
            Jedis jedis = getJedis();
            long status = jedis.renamenx(oldkey, newkey);
            returnJedis(jedis);
            return status;
        }

        /**
         * 更改key
         */
        public String rename(byte[] oldkey, byte[] newkey) {
            Jedis jedis = getJedis();
            String status = jedis.rename(oldkey, newkey);
            returnJedis(jedis);
            return status;
        }


        /**
         * 设置key的过期时间,它是距历元（即格林威治标准时间 1970 年 1 月 1 日的 00:00:00，格里高利历）的偏移量。
         */
        public long expireAt(String key, long timestamp) {
            Jedis jedis = getJedis();
            long count = jedis.expireAt(key, timestamp);
            returnJedis(jedis);
            return count;
        }

        /**
         * 查询key的过期时间
         */
        public long ttl(String key) {
            Jedis sjedis = getJedis();
            long len = sjedis.ttl(key);
            returnJedis(sjedis);
            return len;
        }

        /**
         * 取消对key过期时间的设置
         */
        public long persist(String key) {
            Jedis jedis = getJedis();
            long count = jedis.persist(key);
            returnJedis(jedis);
            return count;
        }


        /**
         * 对List,Set,SortSet进行排序,如果集合数据较大应避免使用这个方法
         */
        public List<String> sort(String key) {
            Jedis sjedis = getJedis();
            List<String> list = sjedis.sort(key);
            returnJedis(sjedis);
            return list;
        }

        /**
         * 对List,Set,SortSet进行排序或limit
         */
        public List<String> sort(String key, SortingParams parame) {
            Jedis sjedis = getJedis();
            List<String> list = sjedis.sort(key, parame);
            returnJedis(sjedis);
            return list;
        }

        /**
         * 返回指定key存储的类型
         */
        public String type(String key) {
            Jedis sjedis = getJedis();
            String type = sjedis.type(key);
            returnJedis(sjedis);
            return type;
        }

        /**
         * 查找所有匹配给定的模式的键
         */
        public Set<String> keys(String pattern) {
            Jedis jedis = getJedis();
            Set<String> set = jedis.keys(pattern);
            returnJedis(jedis);
            return set;
        }

        /**
         * 清楚所有匹配key数据
         *
         * @param pattern
         */
        public void flushKeys(String pattern){
            Jedis jedis = getJedis();
            Set<String> sets = jedis.keys(pattern);
            for(String key:sets){
                del(key);
            }
            returnJedis(jedis);
        }
    }



    //*******************************************Strings*******************************************//
    // Redis 字符串数据类型的相关命令用于管理 redis 字符串值
    public class Strings {

        /**
         * 添加记录,如果记录已存在将覆盖原有的value
         */
        public String set(String key, String value) {
            return set(SafeEncoder.encode(key), SafeEncoder.encode(value));
        }

        /**
         * 添加记录,如果记录已存在将覆盖原有的value
         */
        public String set(String key, byte[] value) {
            return set(SafeEncoder.encode(key), value);
        }

        /**
         * 添加记录,如果记录已存在将覆盖原有的value
         */
        public String set(byte[] key, byte[] value) {
            Jedis jedis = getJedis();
            String status = jedis.set(key, value);
            returnJedis(jedis);
            return status;
        }


        /**
         * 根据key获取记录
         */
        public String get(String key) {
            Jedis sjedis = getJedis();
            String value = sjedis.get(key);
            returnJedis(sjedis);
            return value;
        }

        /**
         * 根据key获取记录
         */
        public byte[] get(byte[] key) {
            Jedis sjedis = getJedis();
            byte[] value = sjedis.get(key);
            returnJedis(sjedis);
            return value;
        }





        /**
         * 添加有过期时间的记录
         */
        public String setEx(String key, int seconds, String value) {
            Jedis jedis = getJedis();
            String str = jedis.setex(key, seconds, value);
            returnJedis(jedis);
            return str;
        }

        /**
         * 添加有过期时间的记录
         */
        public String setEx(byte[] key, int seconds, byte[] value) {
            Jedis jedis = getJedis();
            String str = jedis.setex(key, seconds, value);
            returnJedis(jedis);
            return str;
        }

        /**
         * 添加一条记录，仅当给定的key不存在时才插入
         */
        public long setnx(String key, String value) {
            Jedis jedis = getJedis();
            long str = jedis.setnx(key, value);
            returnJedis(jedis);
            return str;
        }

        /**
         * 从指定位置开始插入数据，插入的数据会覆盖指定位置以后的数据
         */
        public long setRange(String key, long offset, String value) {
            Jedis jedis = getJedis();
            long len = jedis.setrange(key, offset, value);
            returnJedis(jedis);
            return len;
        }

        /**
         * 在指定的key中追加value
         */
        public long append(String key, String value) {
            Jedis jedis = getJedis();
            long len = jedis.append(key, value);
            returnJedis(jedis);
            return len;
        }

        /**
         * 将key对应的value减去指定的值，只有value可以转为数字时该方法才可用
         */
        public long decrBy(String key, long number) {
            Jedis jedis = getJedis();
            long len = jedis.decrBy(key, number);
            returnJedis(jedis);
            return len;
        }

        /**
         * 可以作为获取唯一id的方法
         * 将key对应的value加上指定的值，只有value可以转为数字时该方法才可用
         */
        public long incrBy(String key, long number) {
            Jedis jedis = getJedis();
            long len = jedis.incrBy(key, number);
            returnJedis(jedis);
            return len;
        }

        /**
         * 对指定key对应的value进行截取
         */
        public String getrange(String key, long startOffset, long endOffset) {
            Jedis sjedis = getJedis();
            String value = sjedis.getrange(key, startOffset, endOffset);
            returnJedis(sjedis);
            return value;
        }

        /**
         * 获取并设置指定key对应的value
         * 如果key存在返回之前的value,否则返回null
         */
        public String getSet(String key, String value) {
            Jedis jedis = getJedis();
            String str = jedis.getSet(key, value);
            returnJedis(jedis);
            return str;
        }

        /**
         * 批量获取记录,如果指定的key不存在返回List的对应位置将是null
         */
        public List<String> mget(String... keys) {
            Jedis jedis = getJedis();
            List<String> str = jedis.mget(keys);
            returnJedis(jedis);
            return str;
        }

        /**
         * 批量存储记录
         */
        public String mset(String... keysvalues) {
            Jedis jedis = getJedis();
            String str = jedis.mset(keysvalues);
            returnJedis(jedis);
            return str;
        }

        /**
         * 获取key对应的值的长度
         */
        public long strlen(String key) {
            Jedis jedis = getJedis();
            long len = jedis.strlen(key);
            returnJedis(jedis);
            return len;
        }
    }



    //*******************************************Hash*******************************************//
//    Redis hash 是一个 string 类型的 field（字段） 和 value（值） 的映射表，hash 特别适合用于存储对象。
//    Redis 中每个 hash 可以存储 232 - 1 键值对（40多亿）。
    public class Hashs {

        /**
         * 从hash中删除指定的存储
         *
         * @return 状态码，1成功，0失败
         */
        public long hdel(String key, String fieid) {
            Jedis jedis = getJedis();
            long s = jedis.hdel(key, fieid);
            returnJedis(jedis);
            return s;
        }

        public long hdel(String key) {
            Jedis jedis = getJedis();
            long s = jedis.del(key);
            returnJedis(jedis);
            return s;
        }

        /**
         * 测试hash中指定的存储是否存在
         *
         * @return 1存在，0不存在
         */
        public boolean hexists(String key, String fieid) {
            Jedis sjedis = getJedis();
            boolean s = sjedis.hexists(key, fieid);
            returnJedis(sjedis);
            return s;
        }

        /**
         * 返回hash中指定存储位置的值
         */
        public String hget(String key, String fieid) {
            Jedis sjedis = getJedis();
            String s = sjedis.hget(key, fieid);
            returnJedis(sjedis);
            return s;
        }

        public byte[] hget(byte[] key, byte[] fieid) {
            Jedis sjedis = getJedis();
            byte[] s = sjedis.hget(key, fieid);
            returnJedis(sjedis);
            return s;
        }

        /**
         * 以Map的形式返回hash中的存储和值
         */
        public Map<String, String> hgetAll(String key) {
            Jedis sjedis = getJedis();
            Map<String, String> map = sjedis.hgetAll(key);
            returnJedis(sjedis);
            return map;
        }

        /**
         * 添加一个对应关系
         *
         * @return 状态码 1成功，0失败，fieid已存在将更新，也返回0
         */
        public long hset(String key, String fieid, String value) {
            Jedis jedis = getJedis();
            long s = jedis.hset(key, fieid, value);
            returnJedis(jedis);
            return s;
        }

        public long hset(String key, String fieid, byte[] value) {
            Jedis jedis = getJedis();
            long s = jedis.hset(key.getBytes(), fieid.getBytes(), value);
            returnJedis(jedis);
            return s;
        }

        /**
         * 只有在字段 field 不存在时，设置哈希表字段的值。
         *
         * @return 状态码 1成功，0失败fieid已存
         */
        public long hsetnx(String key, String fieid, String value) {
            Jedis jedis = getJedis();
            long s = jedis.hsetnx(key, fieid, value);
            returnJedis(jedis);
            return s;
        }

        /**
         * 获取hash中value的集合
         */
        public List<String> hvals(String key) {
            Jedis sjedis = getJedis();
            List<String> list = sjedis.hvals(key);
            returnJedis(sjedis);
            return list;
        }

        /**
         * 返回指定hash中的所有存储名字,类似Map中的keySet方法
         */
        public Set<String> hkeys(String key) {
            Jedis sjedis = getJedis();
            Set<String> set = sjedis.hkeys(key);
            returnJedis(sjedis);
            return set;
        }

        /**
         * 在指定的存储位置加上指定的数字，存储位置的值必须可转为数字类型
         */
        public long hincrby(String key, String fieid, long value) {
            Jedis jedis = getJedis();
            long s = jedis.hincrBy(key, fieid, value);
            returnJedis(jedis);
            return s;
        }


        /**
         * 获取hash中存储的个数，类似Map中size方法
         */
        public long hlen(String key) {
            Jedis sjedis = getJedis();
            long len = sjedis.hlen(key);
            returnJedis(sjedis);
            return len;
        }

        /**
         * 根据多个key，获取对应的value，返回List,如果指定的key不存在,List对应位置为null
         */
        public List<String> hmget(String key, String... fieids) {
            Jedis sjedis = getJedis();
            List<String> list = sjedis.hmget(key, fieids);
            returnJedis(sjedis);
            return list;
        }

        public List<byte[]> hmget(byte[] key, byte[]... fieids) {
            Jedis sjedis = getJedis();
            List<byte[]> list = sjedis.hmget(key, fieids);
            returnJedis(sjedis);
            return list;
        }

        /**
         * 添加对应关系，如果对应关系已存在，则覆盖
         */
        public String hmset(String key, Map<String, String> map) {
            Jedis jedis = getJedis();
            String s = jedis.hmset(key, map);
            returnJedis(jedis);
            return s;
        }

        /**
         * 添加对应关系，如果对应关系已存在，则覆盖
         */
        public String hmset(byte[] key, Map<byte[], byte[]> map) {
            Jedis jedis = getJedis();
            String s = jedis.hmset(key, map);
            returnJedis(jedis);
            return s;
        }

    }


    //*******************************************Sets*******************************************//
    public class Sets {

        /**
         * 向Set添加一条记录，如果member已存在返回0,否则返回1
         *
         * @return 操作码, 0或1
         */
        public long sadd(String key, String member) {
            Jedis jedis = getJedis();
            long s = jedis.sadd(key, member);
            returnJedis(jedis);
            return s;
        }

        public long sadd(byte[] key, byte[] member) {
            Jedis jedis = getJedis();
            long s = jedis.sadd(key, member);
            returnJedis(jedis);
            return s;
        }

        /**
         * 获取给定key中元素个数
         *
         * @return 元素个数
         */
        public long scard(String key) {
            Jedis sjedis = getJedis();
            long len = sjedis.scard(key);
            returnJedis(sjedis);
            return len;
        }

        /**
         * 返回从第一组和所有的给定集合之间的差异的成员
         *
         * @return 差异的成员集合
         */
        public Set<String> sdiff(String... keys) {
            Jedis jedis = getJedis();
            Set<String> set = jedis.sdiff(keys);
            returnJedis(jedis);
            return set;
        }

        /**
         * 这个命令等于sdiff,但返回的不是结果集,而是将结果集存储在新的集合中，如果目标已存在，则覆盖。
         *
         * @return 新集合中的记录数
         */
        public long sdiffstore(String newkey, String... keys) {
            Jedis jedis = getJedis();
            long s = jedis.sdiffstore(newkey, keys);
            returnJedis(jedis);
            return s;
        }

        /**
         * 返回给定集合交集的成员,如果其中一个集合为不存在或为空，则返回空Set
         *
         * @return 交集成员的集合
         */
        public Set<String> sinter(String... keys) {
            Jedis jedis = getJedis();
            Set<String> set = jedis.sinter(keys);
            returnJedis(jedis);
            return set;
        }

        /**
         * 这个命令等于sinter,但返回的不是结果集,而是将结果集存储在新的集合中，如果目标已存在，则覆盖。
         *
         * @return 新集合中的记录数
         */
        public long sinterstore(String newkey, String... keys) {
            Jedis jedis = getJedis();
            long s = jedis.sinterstore(newkey, keys);
            returnJedis(jedis);
            return s;
        }

        /**
         * 确定一个给定的值是否存在
         *
         * @return 存在返回1，不存在返回0
         */
        public boolean sismember(String key, String member) {
            Jedis sjedis = getJedis();
            boolean s = sjedis.sismember(key, member);
            returnJedis(sjedis);
            return s;
        }

        /**
         * 返回集合中的所有成员
         *
         * @return 成员集合
         */
        public Set<String> smembers(String key) {
            Jedis sjedis = getJedis();
            Set<String> set = sjedis.smembers(key);
            returnJedis(sjedis);
            return set;
        }

        public Set<byte[]> smembers(byte[] key) {
            Jedis sjedis = getJedis();
            Set<byte[]> set = sjedis.smembers(key);
            returnJedis(sjedis);
            return set;
        }

        /**
         * 将成员从源集合移出放入目标集合 <br/>
         * 如果源集合不存在或不包哈指定成员，不进行任何操作，返回0<br/>
         * 否则该成员从源集合上删除，并添加到目标集合，如果目标集合中成员已存在，则只在源集合进行删除
         *
         * @return 状态码，1成功，0失败
         */
        public long smove(String srckey, String dstkey, String member) {
            Jedis jedis = getJedis();
            long s = jedis.smove(srckey, dstkey, member);
            returnJedis(jedis);
            return s;
        }

        /**
         * 从集合中删除成员
         *
         * @return 被删除的成员
         */
        public String spop(String key) {
            Jedis jedis = getJedis();
            String s = jedis.spop(key);
            returnJedis(jedis);
            return s;
        }

        /**
         * 从集合中删除指定成员
         *
         * @return 状态码，成功返回1，成员不存在返回0
         */
        public long srem(String key, String member) {
            Jedis jedis = getJedis();
            long s = jedis.srem(key, member);
            returnJedis(jedis);
            return s;
        }

        /**
         * 合并多个集合并返回合并后的结果，合并后的结果集合并不保存<br/>
         *
         * @return 合并后的结果集合
         */
        public Set<String> sunion(String... keys) {
            Jedis jedis = getJedis();
            Set<String> set = jedis.sunion(keys);
            returnJedis(jedis);
            return set;
        }

        /**
         * 合并多个集合并将合并后的结果集保存在指定的新集合中，如果新集合已经存在则覆盖
         */
        public long sunionstore(String newkey, String... keys) {
            Jedis jedis = getJedis();
            long s = jedis.sunionstore(newkey, keys);
            returnJedis(jedis);
            return s;
        }
    }

    //*******************************************SortSet*******************************************//
    public class SortSets {

        /**
         * 向集合中增加一条记录,如果这个值已存在，这个值对应的权重将被置为新的权重
         *
         * @return 状态码 1成功，0已存在member的值
         */
        public long zadd(String key, double score, String member) {
            Jedis jedis = getJedis();
            long s = jedis.zadd(key, score, member);
            returnJedis(jedis);
            return s;
        }

        /**
         * 获取集合中元素的数量
         *
         * @return 如果返回0则集合不存在
         */
        public long zcard(String key) {
            Jedis sjedis = getJedis();
            long len = sjedis.zcard(key);
            returnJedis(sjedis);
            return len;
        }

        /**
         * 获取指定权重区间内集合的数量
         */
        public long zcount(String key, double min, double max) {
            Jedis sjedis = getJedis();
            long len = sjedis.zcount(key, min, max);
            returnJedis(sjedis);
            return len;
        }

        /**
         * 获得set的长度
         */
        public long zlength(String key) {
            long len = 0;
            Set<String> set = zrange(key, 0, -1);
            len = set.size();
            return len;
        }

        /**
         * 权重增加给定值，如果给定的member已存在
         *
         * @return 增后的权重
         */
        public double zincrby(String key, double score, String member) {
            Jedis jedis = getJedis();
            double s = jedis.zincrby(key, score, member);
            returnJedis(jedis);
            return s;
        }

        /**
         * 返回指定位置的集合元素,0为第一个元素，-1为最后一个元素
         *
         * @return Set<String>
         */
        public Set<String> zrange(String key, int start, int end) {
            Jedis sjedis = getJedis();
            Set<String> set = sjedis.zrange(key, start, end);
            returnJedis(sjedis);
            return set;
        }

        /**
         * 返回指定权重区间的元素集合
         *
         * @return Set<String>
         */
        public Set<String> zrangeByScore(String key, double min, double max) {
            Jedis sjedis = getJedis();
            Set<String> set = sjedis.zrangeByScore(key, min, max);
            returnJedis(sjedis);
            return set;
        }

        /**
         * 获取指定值在集合中的位置，集合排序从低到高
         */
        public long zrank(String key, String member) {
            Jedis jedis = getJedis();
            long index = -1;
            try {
                index = jedis.zrank(key, member);
            }catch (Exception e){}
            returnJedis(jedis);
            return index;
        }

        /**
         * 返回有序集合
         *
         * @param key
         * @return
         */
        public List<Tuple> zscan(String key){
            Jedis jedis = getJedis();
            ScanResult<Tuple> zscan = jedis.zscan(key, "0");
            List<Tuple> result = zscan.getResult();
            returnJedis(jedis);
            return result;
        }


        /**
         * 获取指定值在集合中的位置，集合排序从高到低
         */
        public long zrevrank(String key, String member) {
            Jedis sjedis = getJedis();
            long index = sjedis.zrevrank(key, member);
            returnJedis(sjedis);
            return index;
        }

        /**
         * 从集合中删除成员
         *
         * @return 返回1成功
         */
        public long zrem(String key, String member) {
            Jedis jedis = getJedis();
            long s = jedis.zrem(key, member);
            returnJedis(jedis);
            return s;
        }

        /**
         * 删除
         */
        public long zrem(String key) {
            Jedis jedis = getJedis();
            long s = jedis.del(key);
            returnJedis(jedis);
            return s;
        }

        /**
         * 删除给定位置区间的元素
         */
        public long zremrangeByRank(String key, int start, int end) {
            Jedis jedis = getJedis();
            long s = jedis.zremrangeByRank(key, start, end);
            returnJedis(jedis);
            return s;
        }

        /**
         * 删除给定权重区间的元素
         *
         * @return 删除的数量
         */
        public long zremrangeByScore(String key, double min, double max) {
            Jedis jedis = getJedis();
            long s = jedis.zremrangeByScore(key, min, max);
            returnJedis(jedis);
            return s;
        }

        /**
         * 获取给定区间的元素，原始按照权重由高到低排序
         */
        public Set<String> zrevrange(String key, int start, int end) {
            Jedis sjedis = getJedis();
            Set<String> set = sjedis.zrevrange(key, start, end);
            returnJedis(sjedis);
            return set;
        }

        /**
         * 获取给定值在集合中的权重
         */
        public double zscore(String key, String memebr) {
            Jedis sjedis = getJedis();
            Double score = sjedis.zscore(key, memebr);
            returnJedis(sjedis);
            if (score != null)
                return score;
            return 0;
        }
    }


    //*******************************************Lists*******************************************//
//    Redis列表是简单的字符串列表，按照插入顺序排序。你可以添加一个元素到列表的头部（左边）或者尾部（右边）,一个列表最多可以包含 232 - 1 个元素 (4294967295, 每个列表超过40亿个元素)。
    public class Lists {
        /**
         * List长度
         */
        public long llen(String key) {
            return llen(SafeEncoder.encode(key));
        }

        /**
         * List长度
         */
        public long llen(byte[] key) {
            Jedis sjedis = getJedis();
            long count = sjedis.llen(key);
            returnJedis(sjedis);
            return count;
        }

        /**
         * 覆盖操作,将覆盖List中指定位置的值
         *
         * @return 状态码
         */
        public String lset(byte[] key, int index, byte[] value) {
            Jedis jedis = getJedis();
            String status = jedis.lset(key, index, value);
            returnJedis(jedis);
            return status;
        }

        /**
         * 覆盖操作,将覆盖List中指定位置的值
         */
        public String lset(String key, int index, String value) {
            return lset(SafeEncoder.encode(key), index, SafeEncoder.encode(value));
        }


//        /**
//         * 在value的相对位置插入记录
//         */
//        public long linsert(String key, LIST_POSITION where, String pivot, String value) {
//            return linsert(SafeEncoder.encode(key), where, SafeEncoder.encode(pivot), SafeEncoder.encode(value));
//        }
//
//        /**
//         * 在指定位置插入记录
//         */
//        public long linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value) {
//            Jedis jedis = getJedis();
//            long count = jedis.linsert(key, where, pivot, value);
//            returnJedis(jedis);
//            return count;
//        }


        /**
         * 获取List中指定位置的值
         */
        public String lindex(String key, int index) {
            return SafeEncoder.encode(lindex(SafeEncoder.encode(key), index));
        }

        /**
         * 获取List中指定位置的值
         */
        public byte[] lindex(byte[] key, int index) {
            Jedis sjedis = getJedis();
            byte[] value = sjedis.lindex(key, index);
            returnJedis(sjedis);
            return value;
        }

        /**
         * 将List中的第一条记录移出List
         */
        public String lpop(String key) {
            return SafeEncoder.encode(lpop(SafeEncoder.encode(key)));
        }

        /**
         * 将List中的第一条记录移出List
         */
        public byte[] lpop(byte[] key) {
            Jedis jedis = getJedis();
            byte[] value = jedis.lpop(key);
            returnJedis(jedis);
            return value;
        }

        /**
         * 将List中最后第一条记录移出List
         */
        public String rpop(String key) {
            Jedis jedis = getJedis();
            String value = jedis.rpop(key);
            returnJedis(jedis);
            return value;
        }

        /**
         * 向List尾部追加记录
         */
        public long lpush(String key, String value) {
            return lpush(SafeEncoder.encode(key), SafeEncoder.encode(value));
        }

        /**
         * 向List头部追加记录
         */
        public long rpush(String key, String value) {
            Jedis jedis = getJedis();
            long count = jedis.rpush(key, value);
            returnJedis(jedis);
            return count;
        }

        /**
         * 向List头部追加记录
         */
        public long rpush(byte[] key, byte[] value) {
            Jedis jedis = getJedis();
            long count = jedis.rpush(key, value);
            returnJedis(jedis);
            return count;
        }

        /**
         * 向List中追加记录
         */
        public long lpush(byte[] key, byte[] value) {
            Jedis jedis = getJedis();
            long count = jedis.lpush(key, value);
            returnJedis(jedis);
            return count;
        }

        /**
         * 获取指定范围的记录，可以做为分页使用
         */
        public List<String> lrange(String key, long start, long end) {
            Jedis sjedis = getJedis();
            List<String> list = sjedis.lrange(key, start, end);
            returnJedis(sjedis);
            return list;
        }

        /**
         * 获取指定范围的记录，可以做为分页使用
         */
        public List<byte[]> lrange(byte[] key, int start, int end) {
            Jedis sjedis = getJedis();
            List<byte[]> list = sjedis.lrange(key, start, end);
            returnJedis(sjedis);
            return list;
        }

        /**
         * 删除List中c条记录，被删除的记录值为value
         */
        public long lrem(byte[] key, int c, byte[] value) {
            Jedis jedis = getJedis();
            long count = jedis.lrem(key, c, value);
            returnJedis(jedis);
            return count;
        }

        /**
         * 删除List中c条记录，被删除的记录值为value
         */
        public long lrem(String key, int c, String value) {
            return lrem(SafeEncoder.encode(key), c, SafeEncoder.encode(value));
        }

        /**
         * 算是删除吧，只保留start与end之间的记录
         */
        public String ltrim(byte[] key, int start, int end) {
            Jedis jedis = getJedis();
            String str = jedis.ltrim(key, start, end);
            returnJedis(jedis);
            return str;
        }

        /**
         * 算是删除吧，只保留start与end之间的记录
         */
        public String ltrim(String key, int start, int end) {
            return ltrim(SafeEncoder.encode(key), start, end);
        }

    }

    public class HyperLogLog{

        /**
         * 添加指定元素到 HyperLogLog 中。
         */
        public Long pfadd(byte[] key,byte[]... values){
            Jedis jedis = getJedis();
            Long pfadd = jedis.pfadd(key, values);
            returnJedis(jedis);
            return pfadd;
        }

        public Long pfadd(String key,String... values){
            Jedis jedis = getJedis();
            Long pfadd = jedis.pfadd(key, values);
            returnJedis(jedis);
            return pfadd;
        }


        /**
         * 返回给定 HyperLogLog 的基数估算值。
         * @param keys
         * @return
         */
        public Long pfcount(String... keys){
            Jedis jedis = getJedis();
            Long pfadd = jedis.pfcount(keys);
            returnJedis(jedis);
            return pfadd;
        }

        /**
         * 将多个 HyperLogLog 合并为一个 HyperLogLog
         * @return
         */
        public String pfmerge(String destKey,String... sourceKeys){
            Jedis jedis = getJedis();
            String pfmerge = jedis.pfmerge(destKey, sourceKeys);
            returnJedis(jedis);
            return pfmerge;
        }

    }


    public class Bitmap {
        /**
         * 设置key对应的offset为true
         *
         * @param key
         * @param offset
         * @param b
         * @return
         */
        public Boolean setBit(String key, Long offset, boolean b){
            Jedis jedis = getJedis();
            Boolean setbit = jedis.setbit(key, offset, true);
            returnJedis(jedis);
            return setbit;
        }


        /**
         * 获取key对应位移是否设置
         *
         * @param key
         * @param offset
         * @return
         */
        public Boolean getBit(String key,Long offset){
            Jedis jedis = getJedis();
            Boolean getbit = jedis.getbit(key, offset);
            returnJedis(jedis);
            return getbit;
        }

        /**
         * 获取key对应的计数
         *
         * @param key
         * @return
         */
        public Long bitCount(String key){
            Jedis jedis = getJedis();
            Long bitcount = jedis.bitcount(key);
            returnJedis(jedis);
            return bitcount;
        }


    }

}
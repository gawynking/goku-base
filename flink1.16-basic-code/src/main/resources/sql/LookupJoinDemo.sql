
-- 源生产者
kafka-console-producer.sh --broker-list localhost:9092 --topic order_source_topic
kafka-console-producer.sh --broker-list localhost:9092 --topic order_payment_source_topic

-- 源消费者
kafka-console-consumer.sh --zookeeper localhost:2181 --topic order_source_topic --from-beginning
kafka-console-consumer.sh --zookeeper localhost:2181 --topic order_payment_source_topic --from-beginning



kafka-topics.sh -delete --zookeeper localhost:2181 --topic order_payment_source_topic
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic order_payment_source_topic
kafka-console-consumer.sh --zookeeper localhost:2181 --topic order_payment_source_topic --from-beginning





-- 订单表建表语句
create table tbl_order_source(
    order_id            int             comment '订单ID',
    shop_id             int             comment '书店ID',
    user_id             int             comment '用户ID',
    original_price      double          comment '原始交易额',
    actual_price        double          comment '实付交易额',
    discount_price      double          comment '折扣金额',
    order_status        int             comment '订单状态: 1-提单 2-支付 3-配送 4-完单 5-取消',
    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss'
--    ,watermark for create_time as create_time - interval '10' second
    ,row_time AS proctime()
)with(
    'connector' = 'kafka',
    'topic' = 'order_source_topic',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'testGroup',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);


create table tbl_user_source(
    user_id           int            comment '用户ID',
    user_name         string         comment '用户名称',
    sex               int            comment '性别',
    account           string         comment '账号',
    nick_name         string         comment '昵称',
    city_id           int            comment '城市ID',
    city_name         string         comment '城市名称',
    crowd_type        string         comment '人群类型',
    register_time     string         comment '注册时间',
    primary key(user_id) not enforced
) with (
        'connector' = 'jdbc',
        'url' = 'jdbc:mysql://localhost:3306/gawyn',
        'driver' = 'com.mysql.jdbc.Driver',
        'username' = 'root',
        'password' = 'mysql',
        'table-name' = 'tbl_user'
);


select

    t1.order_id,
    t1.shop_id,
    t1.user_id,
    t2.user_name,
    t2.city_id,
    t2.city_name,
    t1.original_price,
    t1.create_time

from tbl_order_source t1
left join tbl_user_source for system_time as of t1.row_time t2
     on t1.user_id = t2.user_id
;

tbl_order_source：kafka数据源
tbl_user_source：mysql数据源

FlinkSQL lookup查询外表，能启用异步IO吗？




-- 订单数据
{"order_id":"1","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:00"}
{"order_id":"2","shop_id":"1","user_id":"2","original_price":"20","actual_price":"10","discount_price":"0","order_status":"0","create_time":"2024-03-14 20:00:30"}
{"order_id":"3","shop_id":"1","user_id":"-1","original_price":"20","actual_price":"10","discount_price":"0","order_status":"0","create_time":"2024-03-14 20:00:30"}


------------------------------------------------

kafka-topics.sh -delete --zookeeper localhost:2181 --topic user_upsert_topic
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic user_upsert_topic
kafka-console-consumer.sh --zookeeper localhost:2181 --topic user_upsert_topic --from-beginning


create table tbl_user_upsert(
    user_id             int             comment '用户ID',
    original_price      double          comment '原价金额',
    row_time as proctime(),
    primary key (order_id) not enforced
)with(
    'connector' = 'upsert-kafka',
    'topic' = 'user_upsert_topic',
    'properties.bootstrap.servers' = 'localhost:9092',
    'key.format' = 'json',
    'key.json.ignore-parse-errors' = 'true',
    'value.format' = 'json',
    'value.json.fail-on-missing-field' = 'false'
);


insert into tbl_user_upsert
select
    user_id,
    sum(original_price) as original_price
from tbl_order_source
group by
    user_id
;


select

    t1.user_id,
    t2.user_name,
    t2.city_id,
    t2.city_name,
    t1.original_price

from tbl_user_upsert t1
join tbl_user_source for system_time as of t1.row_time t2
     on t1.user_id = t2.user_id
;


select

    t1.user_id,
    t2.user_name,
    t2.city_id,
    t2.city_name,
    t1.original_price

from tbl_user_upsert t1
join tbl_user_source for system_time as of proctime() t2
     on t1.user_id = t2.user_id
;
-- 订单数据
{"order_id":"1","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:00"}
{"order_id":"2","shop_id":"1","user_id":"2","original_price":"20","actual_price":"10","discount_price":"0","order_status":"0","create_time":"2024-03-14 20:00:30"}
{"order_id":"3","shop_id":"1","user_id":"-1","original_price":"20","actual_price":"10","discount_price":"0","order_status":"0","create_time":"2024-03-14 20:00:30"}



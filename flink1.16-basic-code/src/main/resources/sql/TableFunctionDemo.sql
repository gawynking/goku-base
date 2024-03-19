
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
    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',
    order_tag           string          comment '订单标签集合',
    watermark for create_time as create_time - interval '10' second
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


select
    order_id,
    shop_id,
    user_id,
    tag
from tbl_order_source
join lateral table(exp_order_tag(order_tag)) t(tag) on true
;


{"order_id":"1","order_tag":"a,b,c,d,e","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:00"}








kafka-topics.sh -delete --zookeeper localhost:2181 --topic order_upsert_topic
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic order_upsert_topic



create table tbl_order_upsert(
    order_id            int             comment '订单ID',
    user_id             int             comment '用户ID',
    original_price      double          comment '原始交易额',
    order_tag           string          comment '订单标签集合',
    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',
    watermark for create_time as create_time - interval '10' second,
    primary key(order_id) not enforced
)with(
    'connector' = 'upsert-kafka',
    'topic' = 'order_upsert_topic',
    'properties.bootstrap.servers' = 'localhost:9092',
    'key.format' = 'json',
    'key.json.ignore-parse-errors' = 'true',
    'value.format' = 'json',
    'value.json.fail-on-missing-field' = 'false'
);



insert into tbl_order_upsert
select
    order_id,
    user_id,
    max(original_price) as original_price,
    order_tag,
    max(create_time)    as create_time
from tbl_order_source
group by
    order_id,
    user_id,
    order_tag
;


select
    order_id,
    user_id,
    tag
from tbl_order_upsert
join lateral table(exp_order_tag(order_tag)) t(tag) on true
;


{"order_id":"1","order_tag":"a,b,c,d,e","shop_id":"1","user_id":"2","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:00"}
{"order_id":"1","order_tag":"","shop_id":"1","user_id":"2","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:00"}





create table tbl_order_source(
    order_id            int             comment '订单ID',
    shop_id             int             comment '书店ID',
    user_id             int             comment '用户ID',
    original_price      double          comment '原始交易额',
    actual_price        double          comment '实付交易额',
    discount_price      double          comment '折扣金额',
    order_status        int             comment '订单状态: 1-提单 2-支付 3-配送 4-完单 5-取消',
    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',
    watermark for create_time as create_time - interval '0' second
)with(
    'connector' = 'kafka',
    'topic' = 'tbl_order_source',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'testGroup',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);
-- 源生产者
kafka-console-producer.sh --broker-list localhost:9092 --topic tbl_order_source

create table tbl_order_shop_user(
    user_id             int             comment '用户ID',
    shop_id             int             comment '书店ID',
    original_price      double          comment '',
    primary key(user_id,shop_id) not enforced
)with(
    'connector' = 'upsert-kafka',
    'topic' = 'tbl_order_shop_user',
    'properties.bootstrap.servers' = 'localhost:9092',
    'key.format' = 'json',
    'key.json.ignore-parse-errors' = 'true',
    'value.format' = 'json',
    'value.json.fail-on-missing-field' = 'false'
);

kafka-topics.sh -delete --zookeeper localhost:2181 --topic tbl_order_shop_user
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic tbl_order_shop_user
kafka-console-consumer.sh --zookeeper localhost:2181 --topic tbl_order_shop_user --from-beginning


insert into tbl_order_shop_user
select
    user_id,
    shop_id,
    sum(original_price) as original_price
from tbl_order_source
group by
    user_id,
    shop_id
;

-- 测试1
select
    shop_id,
    redis_set_uv(cast(shop_id as string),cast(user_id as string)) as shop_user_cnt
from tbl_order_shop_user
group by
    shop_id
;

{"order_id":"1","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:00"}
{"order_id":"2","shop_id":"1","user_id":"2","original_price":"20","actual_price":"10","discount_price":"0","order_status":"0","create_time":"2024-03-14 20:00:30"}
{"order_id":"3","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:00"}
{"order_id":"4","shop_id":"1","user_id":"3","original_price":"20","actual_price":"10","discount_price":"0","order_status":"0","create_time":"2024-03-14 20:00:30"}
{"order_id":"5","shop_id":"1","user_id":"5","original_price":"20","actual_price":"10","discount_price":"0","order_status":"0","create_time":"2024-03-14 20:00:30"}
{"order_id":"1","shop_id":"2","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:00"}
{"order_id":"1","shop_id":"2","user_id":"2","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:00"}
{"order_id":"1","shop_id":"2","user_id":"3","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:00"}
{"order_id":"1","shop_id":"2","user_id":"8","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:00"}
{"order_id":"1","shop_id":"2","user_id":"9","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:00"}

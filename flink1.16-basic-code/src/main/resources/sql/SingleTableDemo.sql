-- kafka
kafka-console-producer.sh --broker-list localhost:9092 --topic order_source_topic


kafka-topics.sh -delete --zookeeper localhost:2181 --topic shop_indi_update_topic
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic shop_indi_update_topic
kafka-console-consumer.sh --zookeeper localhost:2181 --topic shop_indi_update_topic --from-beginning



-- 测试数据
{"order_id":"1","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:00"}
{"order_id":"2","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:05"}
{"order_id":"3","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:10"}
{"order_id":"4","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:15"}
{"order_id":"5","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:20"}
{"order_id":"6","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:25"}
{"order_id":"7","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:30"}
{"order_id":"8","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:35"}
{"order_id":"9","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:40"}
{"order_id":"10","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:45"}
{"order_id":"11","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:50"}
{"order_id":"12","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:55"}



{"order_id":"21","shop_id":"2","user_id":"2","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:00"}
{"order_id":"22","shop_id":"2","user_id":"2","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:05"}
{"order_id":"23","shop_id":"2","user_id":"2","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:10"}
{"order_id":"24","shop_id":"2","user_id":"2","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:15"}
{"order_id":"25","shop_id":"2","user_id":"2","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:20"}
{"order_id":"26","shop_id":"2","user_id":"2","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:25"}
{"order_id":"27","shop_id":"2","user_id":"2","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:30"}
{"order_id":"28","shop_id":"2","user_id":"2","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:35"}
{"order_id":"29","shop_id":"2","user_id":"2","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:40"}
{"order_id":"30","shop_id":"2","user_id":"2","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:45"}
{"order_id":"31","shop_id":"2","user_id":"2","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:50"}
{"order_id":"32","shop_id":"2","user_id":"2","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:55"}



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
    watermark for create_time as create_time - interval '0' second
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


create table tbl_shop_indi(
    shop_id             int             comment '书店ID',
    original_price      double          comment '原始交易额',
    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',
    watermark for create_time as create_time - interval '0' second,
    primary key (shop_id) not enforced
)with(
    'connector' = 'upsert-kafka',
    'topic' = 'shop_indi_update_topic',
    'properties.bootstrap.servers' = 'localhost:9092',
    'key.format' = 'json',
    'key.json.ignore-parse-errors' = 'true',
    'value.format' = 'json',
    'value.json.fail-on-missing-field' = 'false'
);


-- 排序
kafka-topics.sh -delete --zookeeper localhost:2181 --topic order_sort_topic
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic order_sort_topic
kafka-console-consumer.sh --zookeeper localhost:2181 --topic order_sort_topic --from-beginning

测试数据:
{"order_id":"1","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:01:00"}
{"order_id":"3","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"3","create_time":"2024-03-14 20:03:00"}
{"order_id":"2","shop_id":"1","user_id":"2","original_price":"10","actual_price":"10","discount_price":"0","order_status":"2","create_time":"2024-03-14 20:02:00"}
{"order_id":"5","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"5","create_time":"2024-03-14 20:05:00"}
{"order_id":"4","shop_id":"1","user_id":"5","original_price":"10","actual_price":"10","discount_price":"0","order_status":"4","create_time":"2024-03-14 20:04:00"}

create table tbl_order_sort(
    order_id            int             comment '订单ID',
    shop_id             int             comment '书店ID',
    user_id             int             comment '用户ID',
    original_price      double          comment '原始交易额',
    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',
    watermark for create_time as create_time - interval '0' second,
    primary key(order_id) not enforced
)with(
    'connector' = 'upsert-kafka',
    'topic' = 'order_sort_topic',
    'properties.bootstrap.servers' = 'localhost:9092',
    'key.format' = 'json',
    'key.json.ignore-parse-errors' = 'true',
    'value.format' = 'json',
    'value.json.fail-on-missing-field' = 'false'
);

select

    order_id,
    shop_id,
    user_id,
    original_price,
    create_time

from (
    select

        order_id,
        shop_id,
        user_id,
        original_price,
        create_time,
        row_number()over(partition by order_id order by create_time desc) as rn

    from tbl_order_source
) t
where t.rn = 1
;

-- 去重
kafka-topics.sh -delete --zookeeper localhost:2181 --topic order_decount_topic
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic order_decount_topic
kafka-console-consumer.sh --zookeeper localhost:2181 --topic order_decount_topic --from-beginning

测试数据:
{"order_id":"1","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:01:00"}
{"order_id":"3","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"3","create_time":"2024-03-14 20:03:00"}
{"order_id":"2","shop_id":"1","user_id":"2","original_price":"10","actual_price":"10","discount_price":"0","order_status":"2","create_time":"2024-03-14 20:02:00"}
{"order_id":"5","shop_id":"1","user_id":"5","original_price":"10","actual_price":"10","discount_price":"0","order_status":"5","create_time":"2024-03-14 20:05:00"}
{"order_id":"4","shop_id":"1","user_id":"5","original_price":"10","actual_price":"10","discount_price":"0","order_status":"4","create_time":"2024-03-14 20:04:00"}

-- distinct 以及 groupby 去重均不支持kafka模式
create table tbl_order_decount(
    user_id             int             comment '用户ID',
    shop_id             int             comment '书店ID'
)with(
    'connector' = 'kafka',
    'topic' = 'order_decount_topic',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'testGroup',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);


create table tbl_order_decount(
    user_id             int             comment '用户ID',
    shop_id             int             comment '书店ID',
    primary key(user_id,shop_id) not enforced
)with(
    'connector' = 'upsert-kafka',
    'topic' = 'order_decount_topic',
    'properties.bootstrap.servers' = 'localhost:9092',
    'key.format' = 'json',
    'key.json.ignore-parse-errors' = 'true',
    'value.format' = 'json',
    'value.json.fail-on-missing-field' = 'false'
);

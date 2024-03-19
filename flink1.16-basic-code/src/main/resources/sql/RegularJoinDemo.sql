
-- 源生产者
kafka-console-producer.sh --broker-list localhost:9092 --topic order_source_topic
kafka-console-producer.sh --broker-list localhost:9092 --topic order_payment_source_topic

-- 源消费者
kafka-console-consumer.sh --zookeeper localhost:2181 --topic order_source_topic --from-beginning
kafka-console-consumer.sh --zookeeper localhost:2181 --topic order_payment_source_topic --from-beginning



kafka-topics.sh -delete --zookeeper localhost:2181 --topic order_payment_source_topic
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic order_payment_source_topic
kafka-console-consumer.sh --zookeeper localhost:2181 --topic order_payment_source_topic --from-beginning


-- 订单数据
{"order_id":"1","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:00"}
{"order_id":"1","shop_id":"1","user_id":"1","original_price":"20","actual_price":"10","discount_price":"0","order_status":"0","create_time":"2024-03-14 20:00:30"}


-- 支付数据
{"pay_id":"1","order_id":"1","payment_status":"1","payment_amount":"10","create_time":"2024-03-14 20:00:10"}
{"pay_id":"1","order_id":"1","payment_status":"1","payment_amount":"10","create_time":"2024-03-14 20:00:12"}

{"pay_id":"2","order_id":"1","payment_status":"0","payment_amount":"10","create_time":"2024-03-14 20:00:35"}





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


create table tbl_order_payment_source(
    pay_id              int             comment '支付ID',
    order_id            int             comment '订单ID',
    payment_amount      double          comment '支付金额',
    payment_status      int             comment '支付状态: 0-未支付 1-支付成功 2-支付失败',
    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',
    watermark for create_time as create_time - interval '0' second
)with(
     'connector' = 'kafka',
     'topic' = 'order_payment_source_topic',
     'properties.bootstrap.servers' = 'localhost:9092',
     'properties.group.id' = 'testGroup',
     'scan.startup.mode' = 'latest-offset',
     'format' = 'json',
     'json.fail-on-missing-field' = 'false',
     'json.ignore-parse-errors' = 'true'
 );


select

    t1.order_id,
    t1.shop_id,
    t1.user_id,
    t2.payment_amount,
    t2.create_time

from tbl_order_source t1
join tbl_order_payment_source t2 on t1.order_id = t2.order_id
;


kafka-topics.sh -delete --zookeeper localhost:2181 --topic order_submit_payment_topic
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic order_submit_payment_topic
kafka-console-consumer.sh --zookeeper localhost:2181 --topic order_submit_payment_topic --from-beginning

create table tbl_order_submit_payment(
    order_id            int             comment '订单ID',
    shop_id             int             comment '书店ID',
    user_id             int             comment '用户ID',
    payment_amount      double          comment '支付金额',
    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',
    watermark for create_time as create_time - interval '0' second
)with(
     'connector' = 'kafka',
     'topic' = 'order_submit_payment_topic',
     'properties.bootstrap.servers' = 'localhost:9092',
     'properties.group.id' = 'testGroup33333',
     'scan.startup.mode' = 'latest-offset',
     'format' = 'json',
     'json.fail-on-missing-field' = 'false',
     'json.ignore-parse-errors' = 'true'
);


-- join结果表
create table tbl_order_submit_payment(
    order_id            int             comment '订单ID',
    shop_id             int             comment '书店ID',
    user_id             int             comment '用户ID',
    payment_amount      double          comment '支付金额',
    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',
    watermark for create_time as create_time - interval '0' second,
    primary key (order_id) not enforced
)with(
    'connector' = 'upsert-kafka',
    'topic' = 'order_submit_payment_topic',
    'properties.bootstrap.servers' = 'localhost:9092',
    'key.format' = 'json',
    'key.json.ignore-parse-errors' = 'true',
    'value.format' = 'json',
    'value.json.fail-on-missing-field' = 'false'
);




create table tbl_order_upsert(
    order_id            int             comment '订单ID',
    original_price      double          comment '原价金额',
    primary key (order_id) not enforced
)with(
    'connector' = 'upsert-kafka',
    'topic' = 'order_submit_upsert_topic',
    'properties.bootstrap.servers' = 'localhost:9092',
    'key.format' = 'json',
    'key.json.ignore-parse-errors' = 'true',
    'value.format' = 'json',
    'value.json.fail-on-missing-field' = 'false'
);


create table tbl_order_payment_upsert(
    order_id            int             comment '订单ID',
    payment_amount      double          comment '支付金额',
    primary key (order_id) not enforced
)with(
    'connector' = 'upsert-kafka',
    'topic' = 'order_payment_upsert_topic',
    'properties.bootstrap.servers' = 'localhost:9092',
    'key.format' = 'json',
    'key.json.ignore-parse-errors' = 'true',
    'value.format' = 'json',
    'value.json.fail-on-missing-field' = 'false'
);

select

    t1.order_id,
    t1.original_price,
    t2.payment_amount

from tbl_order_upsert t1
join tbl_order_payment_upsert t2
    on t1.order_id = t2.order_id
;


insert into tbl_order_upsert
select

    order_id,
    sum(original_price) as original_price

from tbl_order_source
group by order_id
;



insert into tbl_order_payment_upsert
select

    order_id,
    sum(payment_amount) as payment_amount

from tbl_order_payment_source
group by order_id
;



kafka-topics.sh -delete --zookeeper localhost:2181 --topic order_submit_upsert_topic
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic order_submit_upsert_topic
kafka-console-consumer.sh --zookeeper localhost:2181 --topic order_submit_upsert_topic --from-beginning


kafka-topics.sh -delete --zookeeper localhost:2181 --topic order_payment_upsert_topic
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic order_payment_upsert_topic
kafka-console-consumer.sh --zookeeper localhost:2181 --topic order_payment_upsert_topic --from-beginning

-- 订单数据
{"order_id":"1","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:00"}
{"order_id":"1","shop_id":"1","user_id":"1","original_price":"20","actual_price":"10","discount_price":"0","order_status":"0","create_time":"2024-03-14 20:00:30"}


-- 支付数据
{"pay_id":"1","order_id":"1","payment_status":"1","payment_amount":"10","create_time":"2024-03-14 20:00:10"}
{"pay_id":"1","order_id":"1","payment_status":"1","payment_amount":"10","create_time":"2024-03-14 20:00:12"}

{"pay_id":"2","order_id":"1","payment_status":"0","payment_amount":"10","create_time":"2024-03-14 20:00:35"}

order_submit_upsert_topic
order_payment_upsert_topic





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


create table tbl_order_payment_source(
    pay_id              int             comment '支付ID',
    order_id            int             comment '订单ID',
    payment_amount      double          comment '支付金额',
    payment_status      int             comment '支付状态: 0-未支付 1-支付成功 2-支付失败',
    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',
    watermark for create_time as create_time - interval '10' second
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
    t2.payment_amount

from tbl_order_source t1
join tbl_order_payment_source t2
     on t1.order_id = t2.order_id
    and t1.create_time >= t2.create_time - interval '10' minute
    and t1.create_time <= t2.create_time
;


insert into tbl_order_submit_payment
select

    t1.order_id,
    t1.shop_id,
    t1.user_id,
    t2.payment_amount,
    cast(t1.create_time as string) as t1_create_time,
    cast(t2.create_time as string) as t2_create_time,
    cast(current_watermark(t1.create_time) as string) as t1_wm,
    current_watermark(t2.create_time) as t2_wm

from tbl_order_source t1
left join tbl_order_payment_source t2
     on t1.order_id = t2.order_id
    and t1.create_time >= t2.create_time - interval '10' minute
    and t1.create_time <= t2.create_time
;

kafka-topics.sh -delete --zookeeper localhost:2181 --topic order_submit_payment_topic
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic order_submit_payment_topic
kafka-console-consumer.sh --zookeeper localhost:2181 --topic order_submit_payment_topic --from-beginning

create table tbl_order_submit_payment(
    order_id               int             comment '订单ID',
    shop_id                int             comment '书店ID',
    user_id                int             comment '用户ID',
    payment_amount         double          comment '支付金额',
    t1_create_time         string          comment 't1创建时间: yyyy-MM-dd HH:mm:ss',
    t2_create_time         string          comment 't2创建时间: yyyy-MM-dd HH:mm:ss',
    t1_wm                  string          comment 't1水位线: yyyy-MM-dd HH:mm:ss',
    t2_wm                  timestamp(3)    comment 't2水位线: yyyy-MM-dd HH:mm:ss'
)with(
     'connector' = 'kafka',
     'topic' = 'order_submit_payment_topic',
     'properties.bootstrap.servers' = 'localhost:9092',
     'properties.group.id' = 'testGroup',
     'scan.startup.mode' = 'latest-offset',
     'format' = 'json',
     'json.fail-on-missing-field' = 'false',
     'json.ignore-parse-errors' = 'true'
 );



create table tbl_order_submit_payment(
    order_id               int             comment '订单ID',
    shop_id                int             comment '书店ID',
    user_id                int             comment '用户ID',
    payment_amount         double          comment '支付金额',
    t1_create_time         string          comment 't1创建时间: yyyy-MM-dd HH:mm:ss',
    t2_create_time         string          comment 't2创建时间: yyyy-MM-dd HH:mm:ss',
    t1_wm                  string          comment 't1水位线: yyyy-MM-dd HH:mm:ss',
    t2_wm                  timestamp(3)    comment 't2水位线: yyyy-MM-dd HH:mm:ss',
    primary key(order_id) not enforced
)with(
    'connector' = 'upsert-kafka',
    'topic' = 'order_submit_payment_topic',
    'properties.bootstrap.servers' = 'localhost:9092',
    'key.format' = 'json',
    'key.json.ignore-parse-errors' = 'true',
    'value.format' = 'json',
    'value.json.fail-on-missing-field' = 'false'
 );

select

    t1.order_id,
    t1.shop_id,
    t1.user_id,
    t2.payment_amount,
    t1.create_time as t1_create_time,
    t2.create_time as t2_create_time,
    current_watermark(t1.create_time) as t1_wm,
    current_watermark(t2.create_time) as t2_wm

from tbl_order_source t1
left join tbl_order_payment_source t2
     on t1.order_id = t2.order_id
    and t1.create_time between t2.create_time - interval '10' minute and t2.create_time
;



{"order_id":"1","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:00:00"}

{"pay_id":"1","order_id":"1","payment_status":"1","payment_amount":"10","create_time":"2024-03-14 20:00:10"}

{"pay_id":"1","order_id":"1","payment_status":"1","payment_amount":"12","create_time":"2024-03-14 20:00:30"}

{"pay_id":"1","order_id":"1","payment_status":"1","payment_amount":"9","create_time":"2024-03-14 20:12:30"}


{"order_id":"2","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:20:00"}

{"pay_id":"1","order_id":"2","payment_status":"1","payment_amount":"10","create_time":"2024-03-14 20:35:10"}

{"order_id":"3","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:35:00"}

{"pay_id":"1","order_id":"3","payment_status":"1","payment_amount":"10","create_time":"2024-03-14 20:38:10"}

{"order_id":"4","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 22:50:00"}

{"pay_id":"1","order_id":"4","payment_status":"1","payment_amount":"10","create_time":"2024-03-14 22:55:10"}

{"order_id":"5","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 22:51:00"}

{"pay_id":"1","order_id":"5","payment_status":"1","payment_amount":"10","create_time":"2024-03-14 22:55:30"}





insert into tbl_order_submit_payment
select

    t1.order_id,
    t1.shop_id,
    t1.user_id,
    t2.payment_amount,
    t2.create_time

from tbl_order_source t1
join tbl_order_payment_source t2
     on t1.order_id = t2.order_id
    and t1.create_time >= t2.create_time - interval '10' minute
    and t1.create_time <= t2.create_time
;

kafka-topics.sh -delete --zookeeper localhost:2181 --topic order_submit_payment_topic
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic order_submit_payment_topic
kafka-console-consumer.sh --zookeeper localhost:2181 --topic order_submit_payment_topic --from-beginning




select

    t1.order_id,
    t1.original_price,
    t2.payment_amount,

from tbl_order_upsert t1
join tbl_order_payment_upsert t2
     on t1.order_id = t2.order_id
    and t1.create_time >= t2.create_time - interval '10' minute
    and t1.create_time <= t2.create_time
;



kafka-topics.sh -delete --zookeeper localhost:2181 --topic order_payment_source_topic
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic order_payment_source_topic
kafka-console-consumer.sh --zookeeper localhost:2181 --topic order_payment_source_topic --from-beginning





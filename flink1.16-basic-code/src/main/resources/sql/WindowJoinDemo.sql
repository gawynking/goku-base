


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
    t1.original_price,
    t2.payment_amount,
    coalesce(t1.window_start, t2.window_start) as window_start,
    coalesce(t1.window_end, t2.window_end) as window_end

from (
    select *
    from table(tumble(table tbl_order_source,descriptor(create_time),interval '5' minutes))
) t1
left join (
    select *
    from table(tumble(table tbl_order_payment_source,descriptor(create_time),interval '5' minutes))
) t2
     on t1.order_id = t2.order_id
    and t1.window_start = t2.window_start
    and t1.window_end = t2.window_end
;


-- 订单数据
{"order_id":"1","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:01:00"}
{"order_id":"2","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:08:00"}
{"order_id":"3","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:08:00"}
{"order_id":"3","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:20:00"}


-- 支付数据
{"pay_id":"1","order_id":"1","payment_status":"1","payment_amount":"10","create_time":"2024-03-14 20:02:10"}
{"pay_id":"2","order_id":"2","payment_status":"1","payment_amount":"10","create_time":"2024-03-14 20:09:12"}
{"pay_id":"2","order_id":"2","payment_status":"1","payment_amount":"10","create_time":"2024-03-14 20:09:18"}



kafka-topics.sh -delete --zookeeper localhost:2181 --topic order_source_topic1
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic order_source_topic1

kafka-topics.sh -delete --zookeeper localhost:2181 --topic order_payment_source_topic1
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic order_payment_source_topic1

create table tbl_order_source1(
    order_id            int             comment '订单ID',
    original_price      double          comment '原始交易额',
    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',
    watermark for create_time as create_time - interval '10' second,
    primary key(order_id) not enforced
)with(
    'connector' = 'upsert-kafka',
    'topic' = 'order_source_topic1',
    'properties.bootstrap.servers' = 'localhost:9092',
    'key.format' = 'json',
    'key.json.ignore-parse-errors' = 'true',
    'value.format' = 'json',
    'value.json.fail-on-missing-field' = 'false'
);


create table tbl_order_payment_source1(
    order_id            int             comment '订单ID',
    payment_amount      double          comment '支付金额',
    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',
    watermark for create_time as create_time - interval '10' second,
    primary key(order_id) not enforced
)with(
    'connector' = 'upsert-kafka',
    'topic' = 'order_payment_source_topic1',
    'properties.bootstrap.servers' = 'localhost:9092',
    'key.format' = 'json',
    'key.json.ignore-parse-errors' = 'true',
    'value.format' = 'json',
    'value.json.fail-on-missing-field' = 'false'
);


insert into tbl_order_source1
select

    order_id,
    sum(original_price) as original_price,
    max(create_time)    as create_time

from tbl_order_source
group by order_id
;


insert into tbl_order_payment_source1
select

    order_id,
    sum(payment_amount) as payment_amount,
    max(create_time)    as create_time

from tbl_order_payment_source
group by order_id
;



select

    t1.order_id,
    t1.original_price,
    t2.payment_amount,
    coalesce(t1.window_start, t2.window_start) as window_start,
    coalesce(t1.window_end, t2.window_end) as window_end

from (
    select *
    from table(tumble(table tbl_order_source1,descriptor(create_time),interval '5' minutes))
) t1
left join (
    select *
    from table(tumble(table tbl_order_payment_source1,descriptor(create_time),interval '5' minutes))
) t2
     on t1.order_id = t2.order_id
    and t1.window_start = t2.window_start
    and t1.window_end = t2.window_end
;



-- 订单数据
{"order_id":"1","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:01:00"}
{"order_id":"2","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:08:00"}
{"order_id":"3","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:08:00"}
{"order_id":"3","shop_id":"1","user_id":"1","original_price":"10","actual_price":"10","discount_price":"0","order_status":"1","create_time":"2024-03-14 20:20:00"}


-- 支付数据
{"pay_id":"1","order_id":"1","payment_status":"1","payment_amount":"10","create_time":"2024-03-14 20:02:10"}
{"pay_id":"2","order_id":"2","payment_status":"1","payment_amount":"10","create_time":"2024-03-14 20:09:12"}
{"pay_id":"2","order_id":"2","payment_status":"1","payment_amount":"10","create_time":"2024-03-14 20:09:18"}


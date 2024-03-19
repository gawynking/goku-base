
-- demo1: 保序任务后的聚合流开发Demo
-- kafka数据源定义：
kafka-topics.sh -delete --zookeeper localhost:2181 --topic tbl_order_source
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic tbl_order_source
## kafka-console-consumer.sh --zookeeper localhost:2181 --topic tbl_order_source --from-beginning
kafka-console-producer.sh --broker-list localhost:9092 --topic tbl_order_source

create table tbl_order_source(
    order_id            int             comment '订单ID',
    shop_id             int             comment '书店ID',
    user_id             int             comment '用户ID',
    original_price      double          comment '原始交易额',
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

-- 数据源数据示例：
{"order_id":"1","shop_id":"1","user_id":"1","original_price":"1","create_time":"2024-01-01 20:05:00"}
{"order_id":"2","shop_id":"1","user_id":"2","original_price":"2","create_time":"2024-01-01 20:04:00"}
{"order_id":"1","shop_id":"1","user_id":"1","original_price":"3","create_time":"2024-01-01 20:03:00"}
{"order_id":"3","shop_id":"1","user_id":"3","original_price":"4","create_time":"2024-01-01 20:02:00"}
{"order_id":"1","shop_id":"1","user_id":"1","original_price":"5","create_time":"2024-01-01 20:04:00"}


-- 保序结果中间表：
kafka-topics.sh -delete --zookeeper localhost:2181 --topic ods_order_source
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ods_order_source
kafka-console-consumer.sh --zookeeper localhost:2181 --topic ods_order_source --from-beginning

create table ods_order_source(
    order_id            int             comment '订单ID',
    shop_id             int             comment '书店ID',
    user_id             int             comment '用户ID',
    original_price      double          comment '订单金额',
    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',
    watermark for create_time as create_time - interval '0' second,
    primary key (order_id) not enforced
)with(
    'connector' = 'upsert-kafka',
    'topic' = 'ods_order_source',
    'properties.bootstrap.servers' = 'localhost:9092',
    'key.format' = 'json',
    'key.json.ignore-parse-errors' = 'true',
    'value.format' = 'json',
    'value.json.fail-on-missing-field' = 'false'
);


-- 保序任务
insert into ods_order_source
select
	tmp.order_id,
	tmp.shop_id,
	tmp.user_id,
	tmp.original_price,
	tmp.create_time
from (
	select
	    t.order_id,
	    t.shop_id,
	    t.user_id,
	    t.original_price,
	    t.create_time,
		row_number()over(partition by t.order_id order by t.create_time asc) as rn
	from tbl_order_source t
) tmp
where tmp.rn = 1
;

-- 设计聚合任务
select
    t.shop_id                                  as shop_id,
    to_date(cast(t.create_time as string))     as create_date,
    sum(t.original_price)                      as original_amt,
    sum(1)                                     as order_num,
    count(distinct t.order_id)                 as order_cnt
from ods_order_source t
group by
    t.shop_id,
    to_date(cast(t.create_time as string))
;

-- 针对源的聚合
select
    t.shop_id                                  as shop_id,
    to_date(cast(t.create_time as string))     as create_date,
    sum(t.original_price)                      as original_amt,
    sum(1)                                     as order_num,
    count(distinct t.order_id)                 as order_cnt
from tbl_order_source t
group by
    t.shop_id,
    to_date(cast(t.create_time as string))
;

-- --------------------------------------------------------------------------------------
-- demo2: 保序任务后的Join流开发Demo
-- kafka数据源定义：
kafka-topics.sh -delete --zookeeper localhost:2181 --topic tbl_order_source
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic tbl_order_source
## kafka-console-consumer.sh --zookeeper localhost:2181 --topic tbl_order_source --from-beginning
kafka-console-producer.sh --broker-list localhost:9092 --topic tbl_order_source

create table tbl_order_source(
    order_id            int             comment '订单ID',
    shop_id             int             comment '书店ID',
    user_id             int             comment '用户ID',
    original_price      double          comment '原始交易额',
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




-- 保序结果中间表：
kafka-topics.sh -delete --zookeeper localhost:2181 --topic ods_order_source
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ods_order_source
kafka-console-consumer.sh --zookeeper localhost:2181 --topic ods_order_source --from-beginning

create table ods_order_source(
    order_id            int             comment '订单ID',
    shop_id             int             comment '书店ID',
    user_id             int             comment '用户ID',
    original_price      double          comment '订单金额',
    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',
    watermark for create_time as create_time - interval '0' second,
    primary key (order_id) not enforced
)with(
    'connector' = 'upsert-kafka',
    'topic' = 'ods_order_source',
    'properties.bootstrap.servers' = 'localhost:9092',
    'key.format' = 'json',
    'key.json.ignore-parse-errors' = 'true',
    'value.format' = 'json',
    'value.json.fail-on-missing-field' = 'false'
);


insert into ods_order_source
select
	tmp.order_id,
	tmp.shop_id,
	tmp.user_id,
	tmp.original_price,
	tmp.create_time
from (
	select
	    t.order_id,
	    t.shop_id,
	    t.user_id,
	    t.original_price,
	    t.create_time,
		row_number()over(partition by t.order_id order by t.create_time asc) as rn
	from tbl_order_source t
) tmp
where tmp.rn = 1
;


-- kafka数据源定义：
kafka-topics.sh -delete --zookeeper localhost:2181 --topic tbl_order_payment_source
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic tbl_order_payment_source
## kafka-console-consumer.sh --zookeeper localhost:2181 --topic tbl_order_payment_source --from-beginning
kafka-console-producer.sh --broker-list localhost:9092 --topic tbl_order_payment_source

create table tbl_order_payment_source(
    order_id            int             comment '订单ID',
    payment_amount      double          comment '支付金额',
    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',
    watermark for create_time as create_time - interval '0' second
)with(
     'connector' = 'kafka',
     'topic' = 'tbl_order_payment_source',
     'properties.bootstrap.servers' = 'localhost:9092',
     'properties.group.id' = 'testGroup',
     'scan.startup.mode' = 'latest-offset',
     'format' = 'json',
     'json.fail-on-missing-field' = 'false',
     'json.ignore-parse-errors' = 'true'
 );




-- 保序结果中间表：
kafka-topics.sh -delete --zookeeper localhost:2181 --topic ods_order_payment_source
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ods_order_payment_source
kafka-console-consumer.sh --zookeeper localhost:2181 --topic ods_order_payment_source --from-beginning

create table ods_order_payment_source(
    order_id            int             comment '订单ID',
    payment_amount      double          comment '支付金额',
    create_time         timestamp(3)    comment '创建时间: yyyy-MM-dd HH:mm:ss',
    watermark for create_time as create_time - interval '0' second,
    primary key (order_id) not enforced
)with(
    'connector' = 'upsert-kafka',
    'topic' = 'ods_order_payment_source',
    'properties.bootstrap.servers' = 'localhost:9092',
    'key.format' = 'json',
    'key.json.ignore-parse-errors' = 'true',
    'value.format' = 'json',
    'value.json.fail-on-missing-field' = 'false'
);


insert into ods_order_payment_source
select
	tmp.order_id,
	tmp.payment_amount,
	tmp.create_time
from (
	select
	    t.order_id,
	    t.payment_amount,
	    t.create_time,
		row_number()over(partition by t.order_id order by t.create_time asc) as rn
	from tbl_order_payment_source t
) tmp
where tmp.rn = 1
;


select

    t1.order_id,
    t1.shop_id,
    t1.user_id,
    t1.original_price,
    t2.payment_amount

from ods_order_source t1
left join ods_order_payment_source t2
     on t1.order_id = t2.order_id
;


-- 数据源数据示例：
{"order_id":"1","shop_id":"1","user_id":"1","original_price":"1","create_time":"2024-01-01 20:05:00"}
{"order_id":"2","shop_id":"1","user_id":"2","original_price":"2","create_time":"2024-01-01 20:04:00"}
{"order_id":"1","shop_id":"1","user_id":"1","original_price":"3","create_time":"2024-01-01 20:03:00"}
{"order_id":"3","shop_id":"1","user_id":"3","original_price":"4","create_time":"2024-01-01 20:02:00"}
{"order_id":"1","shop_id":"1","user_id":"1","original_price":"5","create_time":"2024-01-01 20:04:00"}


{"order_id":"9","payment_amount":"2","create_time":"2024-01-01 20:04:30"}
{"order_id":"1","payment_amount":"5","create_time":"2024-01-01 20:03:30"}
{"order_id":"3","payment_amount":"4","create_time":"2024-01-01 20:02:30"}
{"order_id":"1","payment_amount":"3","create_time":"2024-01-01 20:03:20"}


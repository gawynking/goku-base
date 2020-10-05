
-- Flink SQL 订单表建表语句

-- 指定字段形式 ：
create table tbl_order_source(
id               int    comment '主键',
customer_id      int    comment '客户id',
order_status     int    comment '订单状态',
order_amt        int    comment '订单金额',
create_time      string comment '记录创建时间 : yyyy-MM-dd HH:mm:ss',
row_time as to_timestamp(create_time),
watermark for row_time as row_time - interval '5' second
) with (
'connector.type' = 'kafka',
'connector.version' = '0.11',
'connector.topic' = 'order_topic',
'connector.properties.zookeeper.connect' = 'test.server:2181',
'connector.properties.bootstrap.servers' = 'test.server:9092',
'connector.properties.group.id' = 'table-source-kafka-group',
'connector.startup-mode' = 'latest-offset',

'format.type' = 'json'
)


-- 定义源表 ：

create table tbl_order_source(

json string comment '事件日志',
row_time as to_timestamp(get_json_object(json,'$.create_time')),
watermark for row_time as row_time - interval '5' second

) with (
'connector.type' = 'kafka',
'connector.version' = '0.11',
'connector.topic' = 'order_topic',
'connector.properties.zookeeper.connect' = 'test.server:2181',
'connector.properties.bootstrap.servers' = 'test.server:9092',
'connector.properties.group.id' = 'table-source-kafka-group',
'connector.startup-mode' = 'group-offset',

'format.type' = 'csv',
'format.field-delimiter' = '\t'
)


-- 定义结果表 ：

create table tbl_order_sink(
id               string    comment '主键',
customer_id      string    comment '客户id',
order_status     string    comment '订单状态',
order_amt        string    comment '订单金额',
create_time      string    comment '记录创建时间 : yyyy-MM-dd HH:mm:ss'
) with (
'connector.type' = 'kafka',
'connector.version' = '0.11',
'connector.topic' = 'pgman',
'connector.properties.zookeeper.connect' = 'test.server:2181',
'connector.properties.bootstrap.servers' = 'test.server:9092',
'connector.properties.group.id' = 'testGroup-00',
'connector.startup-mode' = 'group-offset',

'format.type' = 'json'
)


-- 查询语句
insert into tbl_order_sink

select

get_json_object(t1.json,'$.id') as id,
get_json_object(t1.json,'$.customer_id') as customer_id,
get_json_object(t1.json,'$.order_status') as order_status,
get_json_object(t1.json,'$.order_amt') as order_amt,
get_json_object(t1.json,'$.create_time') as create_time

from tbl_order_source t1
where get_json_object(t1.json,'$.order_status') = '4'





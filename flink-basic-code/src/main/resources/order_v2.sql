
-- 设置时间语义,取值如下 : rowtime | proctime(default)
set flink.time.characteristic = proctime;


-- 注册UDF
create function get_json_object as com.pgman.goku.table.udf.ScalarGetJSONItem;


-- 定义源表建表语句
create table tbl_order_source(

json string comment '事件日志'

) with (
'connector.type' = 'kafka',
'connector.version' = '0.11',
'connector.topic' = 'order_topic',
'connector.properties.zookeeper.connect' = 'test.server:2181',
'connector.properties.bootstrap.servers' = 'test.server:9092',
'connector.properties.group.id' = 'table-source-kafka-group100020',
'connector.startup-mode' = 'group-offsets',

'format.type' = 'csv',
'format.field-delimiter' = '\t'
)
;


-- 定义目标表建表语句
create table tbl_order_sink(
customer_id         string    comment '客户id',
order_amt           int       comment '客户下单总金额'
) with (
'connector.type' = 'jdbc',
'connector.url' = 'jdbc:mysql://127.0.0.1:3306/test',
'connector.table' = 'tbl_order_sink',
'connector.driver' = 'com.mysql.jdbc.Driver',
'connector.username' = 'root',
'connector.password' = 'mysql',
'connector.read.fetch-size' = '1',
'connector.write.flush.max-rows' = '1',
'connector.write.flush.interval' = '1s'
)
;



-- 定义加工逻辑
insert into tbl_order_sink on mode upsert(customer_id)
select

get_json_object(t1.json,'$.customer_id') as customer_id,
sum(cast(get_json_object(t1.json,'$.order_amt') as int)) as order_amt

from tbl_order_source t1
where get_json_object(t1.json,'$.order_status') = '4'
group by get_json_object(t1.json,'$.customer_id')
;


insert into tbl_order_sink on mode upsert(customer_id)
select

get_json_object(t1.json,'$.customer_id') as customer_id,
sum(cast(get_json_object(t1.json,'$.order_amt') as int)) as order_amt

from tbl_order_source t1
where get_json_object(t1.json,'$.order_status') = '1'
group by get_json_object(t1.json,'$.customer_id')
;


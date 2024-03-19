


-- ---------------------------------------------------------------------------------------------------------------------
-- 1.定义kafka连接器源表
-- 1.1 定义书店表
create table tbl_book_shop(
    shop_id        int          comment '书店ID',
    shop_name      string       comment '书店名称',
    shop_address   string       comment '书店地址',
    level_id       int          comment '书店等级',
    level_name     string       comment '书店等级名称',
    city_id        int          comment '城市ID',
    city_name      string       comment '城市名称',
    open_date      string       comment '开店日期',
    brand_id       int          comment '品牌ID',
    brand_name     string       comment '品牌名称',
    manager_id     int          comment '经理ID',
    manager_name   int          comment '经理名称',
    create_time    timestamp(3) comment '创建时间',
    primary key(shop_id) not enforced, -- 定义主键约束，但不做强校验
    watermark for create_time as create_time -- 通过watermark定义事件时间
) with (
     'connector' = 'kafka',
     'properties.bootstrap.servers' = 'localhost:9092',
     'topic' = 'shop_topic',
     'properties.group.id' = 'testGroup',
     'scan.startup.mode' = 'earliest-offset',
     'format' = 'json',
     'json.fail-on-missing-field' = 'false',
     'json.ignore-parse-errors' = 'true'
)
;


create table tbl_book_shop(
    shop_id        int          comment '书店ID',
    shop_name      varchar(64)  comment '书店名称',
    shop_address   varchar(64)  comment '书店地址',
    level_id       int          comment '书店等级',
    level_name     varchar(64)  comment '书店等级名称',
    city_id        int          comment '城市ID',
    city_name      varchar(64)  comment '城市名称',
    open_date      varchar(64)  comment '开店日期',
    brand_id       int          comment '品牌ID',
    brand_name     varchar(64)  comment '品牌名称',
    manager_id     int          comment '经理ID',
    manager_name   int          comment '经理名称',
    create_time    datetime     comment '创建时间',
    primary key(shop_id)
);



create table tbl_book_shop_mysql(
    shop_id        int          comment '书店ID',
    shop_name      string       comment '书店名称',
    shop_address   string       comment '书店地址',
    level_id       int          comment '书店等级',
    level_name     string       comment '书店等级名称',
    city_id        int          comment '城市ID',
    city_name      string       comment '城市名称',
    open_date      string       comment '开店日期',
    brand_id       int          comment '品牌ID',
    brand_name     string       comment '品牌名称',
    manager_id     int          comment '经理ID',
    manager_name   int          comment '经理名称',
    create_time    timestamp(3) comment '创建时间',
    primary key(shop_id) not enforced, -- 定义主键约束，但不做强校验
    watermark for create_time as create_time -- 通过watermark定义事件时间
) with (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://localhost:3306/chavin',
   'driver' = 'com.mysql.jdbc.Driver',
   'username' = 'root',
   'password' = 'mysql',
   'table-name' = 'tbl_book_shop'
)


insert into tbl_book_shop_mysql
select
shop_id      ,
shop_name    ,
shop_address ,
level_id     ,
level_name   ,
city_id      ,
city_name    ,
open_date    ,
brand_id     ,
brand_name   ,
manager_id   ,
manager_name ,
create_time
from tbl_book_shop



select

    t1.order_id,
    t1.shop_id,
    t1.user_id,
    t2.shop_name,
    t2.city_id,
    t2.city_name

from tbl_order t1
left join tbl_book_shop_mysql for system_time as of t1.create_time as t2 on t1.shop_id = t2.shop_id
where t1.shop_id <= 10
;



select

    t1.order_id,
    t1.shop_id,
    t1.user_id,
    t2.shop_name,
    t2.city_id,
    t2.city_name

from tbl_order t1
left join tbl_book_shop_mysql t2 on t1.shop_id = t2.shop_id
where t1.shop_id <= 10
;


-- 1.2 定义图书表
create table tbl_book(
    book_id        int            comment '图书ID',
    book_name      string         comment '图书名称',
    price          double         comment '图书售价',
    category_id    int            comment '品类ID',
    category_name  string         comment '品类名称',
    author         string         comment '作者',
    publisher      string         comment '出版社',
    publisher_date string         comment '出版日期',
    create_time    timestamp(3)   comment '创建时间',
    primary key(book_id) not enforced, -- 定义主键约束，但不做强校验
    watermark for create_time as create_time -- 通过watermark定义事件时间
) with (
      'connector' = 'kafka',
      'properties.bootstrap.servers' = 'localhost:9092',
      'topic' = 'book_topic',
      'properties.group.id' = 'testGroup',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
)
;

-- 1.3 定义用户表
create table tbl_user(
    user_id           int            comment '用户ID',
    user_name         string         comment '用户名称',
    sex               int            comment '性别',
    account           string         comment '账号',
    nick_name         string         comment '昵称',
    city_id           int            comment '城市ID',
    city_name         string         comment '城市名称',
    crowd_type        string         comment '人群类型',
    register_time     timestamp(3)   comment '注册时间',
    primary key(user_id) not enforced, -- 定义主键约束，但不做强校验
    watermark for register_time as register_time -- 通过watermark定义事件时间
) with (
        'connector' = 'kafka',
        'properties.bootstrap.servers' = 'localhost:9092',
        'topic' = 'user_topic',
        'properties.group.id' = 'testGroup',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
)
;

-- 1.4 定义订单表
create table tbl_order(
    order_id            int             comment '订单ID',
    shop_id             int             comment '书店ID',
    user_id             int             comment '用户ID',
    original_price      double          comment '原始交易额',
    actual_price        double          comment '实付交易额',
    discount_price      double          comment '折扣金额',
    create_time         timestamp(3)    comment '下单时间',
    -- 声明 create_time 是事件时间属性，并且用 延迟 5 秒的策略来生成 watermark
    watermark for create_time as create_time - interval '5' second
) with (
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'localhost:9092',
    'topic' = 'order_topic',
    'properties.group.id' = 'testGroup',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
)
;

-- 1.5 定义订单详情表
create table tbl_order_detail(
    order_book_id           int            comment '订单明细ID',
    order_id                int            comment '订单ID',
    book_id                 int            comment '图书ID',
    book_number             int            comment '图书下单数量',
    original_price          double         comment '原始交易额',
    actual_price            double         comment '实付交易额',
    discount_price          double         comment '折扣金额',
    create_time             timestamp(3)   comment '下单时间',
    -- 声明 create_time 是事件时间属性，并且用 延迟 5 秒的策略来生成 watermark
    watermark for create_time as create_time - interval '5' second
) with (
     'connector' = 'kafka',
     'properties.bootstrap.servers' = 'localhost:9092',
     'topic' = 'order_book_topic',
     'properties.group.id' = 'testGroup',
     'scan.startup.mode' = 'earliest-offset',
     'format' = 'json',
     'json.fail-on-missing-field' = 'false',
     'json.ignore-parse-errors' = 'true'
)
;


-- ---------------------------------------------------------------------------------------------------------------------
-- 2.定义目标表
-- 2.1 普通kafka sink表
create table tbl_shop_indi_for_kafka(
    create_hour         string    comment '数据时间-小时',
    shop_id             int       comment '书店ID',
    original_amt        double    comment '总原始交易额',
    actual_amt          double    comment '总实付交易额',
    discount_amt        double    comment '总折扣金额',
    order_cnt           bigint    comment '订单数'
) with (
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'localhost:9092',
    'topic' = 'tbl_shop_indi',
    'properties.group.id' = 'testGroup',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
)
;



-- 2.2 jdbc连接器sink表
create table tbl_shop_indi(
    create_hour         string    comment '数据时间-小时',
    shop_id             int       comment '书店ID',
    original_amt        double    comment '总原始交易额',
    actual_amt          double    comment '总实付交易额',
    discount_amt        double    comment '总折扣金额',
    order_cnt           bigint    comment '订单数',
    PRIMARY KEY (create_hour,shop_id) NOT ENFORCED
) with (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://localhost:3306/chavin',
   'driver' = 'com.mysql.jdbc.Driver',
   'username' = 'root',
   'password' = 'mysql',
   'table-name' = 'tbl_shop_indi',
   'sink.buffer-flush.max-rows' = '100',
   'sink.buffer-flush.interval' = '3s',
   'sink.max-retries' = '3'
)
;

drop table tbl_shop_indi;
create table tbl_shop_indi(
    create_hour         varchar(16)    comment '数据时间-小时',
    shop_id             int       comment '书店ID',
    original_amt        double    comment '总原始交易额',
    actual_amt          double    comment '总实付交易额',
    discount_amt        double    comment '总折扣金额',
    order_cnt           int       comment '订单量',
    update_time         datetime  default current_timestamp on update current_timestamp,
    PRIMARY KEY (create_hour,shop_id)
)
;



create table tbl_window_order_indi(
    window_start         timestamp(3)   comment '窗口开始时间',
    window_end           timestamp(3)   comment '窗口结束时间',
    window_time          timestamp(3)   comment '窗口时间',
    original_amt         double         comment '总原始交易额',
    actual_amt           double         comment '总实付交易额',
    order_cnt            int            comment '订单量',
    PRIMARY KEY (window_start,window_end,window_time) NOT ENFORCED
) with (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://localhost:3306/chavin',
   'driver' = 'com.mysql.jdbc.Driver',
   'username' = 'root',
   'password' = 'mysql',
   'table-name' = 'tbl_window_order_indi',
   'sink.buffer-flush.max-rows' = '100',
   'sink.buffer-flush.interval' = '3s',
   'sink.max-retries' = '3'
)
;


drop table tbl_window_order_indi;
create table tbl_window_order_indi(
    window_start         datetime not null      comment '窗口开始时间',
    original_amt         double         comment '总原始交易额',
    actual_amt           double         comment '总实付交易额',
    order_cnt            int            comment '订单量',
    update_time          datetime       default current_timestamp on update current_timestamp,
    PRIMARY KEY (window_start)
)
;

-- ---------------------------------------------------------------------------------------------------------------------
-- 4.定义转换逻辑
-- 1、插入MySQL逻辑
insert into tbl_shop_indi
select

    substr(create_time,1,13) as create_hour,
    shop_id                  as shop_id,
    sum(original_price)      as original_amt,
    sum(actual_price)        as actual_amt,
    sum(discount_price)      as discount_amt,
    count(distinct order_id) as order_cnt

from tbl_order
where shop_id <= 10
  and substr(create_time,1,13) >= '2023-06-03 10'
group by
    shop_id,
    substr(create_time,1,13)
;

-- 2、插入kafka逻辑
insert into tbl_shop_indi_for_kafka
select

    substr(create_time,1,13) as create_hour,
    shop_id                  as shop_id,
    original_price           as original_amt,
    actual_price             as actual_amt,
    discount_price           as discount_amt,
    order_id                 as order_cnt

from tbl_order
where shop_id <= 10
  and substr(create_time,1,13) >= '2023-06-03 16'
;


-- 3、join逻辑
select



from tbl_order t1
left join tbl_order_detail t2 on t1.order_id = t2.order_id
and
;


-- 4、窗口函数
select *
from table(tumble(table tbl_order,descriptor(create_time),interval '10' second))
where create_time >= '2023-06-04 15:00:00'
;


insert into tbl_window_order_indi
select
    window_start,
    sum(original_price) as original_amt,
    sum(actual_price) as actual_amt,
    count(distinct order_id) as order_cnt
from table(tumble(TABLE tbl_order,descriptor(create_time),interval '10' second))
where create_time >= '2023-06-04 15:50:00'
group by
    window_start
;

select
    window_start,
    window_time,
    sum(original_price) as original_amt,
    sum(actual_price) as actual_amt,
    count(distinct order_id) as order_cnt
from table(tumble(TABLE tbl_order,descriptor(create_time),interval '10' second))
where create_time >= '2023-06-04 15:50:00'
group by
    window_start,
    window_time

select t1.shop_id,t1.order_id,t2.window_start,t2.original_amt,t2.actual_amt
from tbl_order t1
join tbl_shop_indi t2 FOR SYSTEM_TIME AS OF t1.create_time on t1.shop_id = t2.shop_id
where t1.shop_id <= 10
;



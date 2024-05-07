

create table tbl_order_detail(
    order_book_id           int            comment '订单明细ID',
    order_id                int            comment '订单ID',
    book_id                 int            comment '图书ID',
    book_number             int            comment '图书下单数量',
    original_price          double         comment '原始交易额',
    actual_price            double         comment '实付交易额',
    discount_price          double         comment '折扣金额',
    create_time             string         comment '下单时间',
    update_time             bigint         comment '更新时间戳'
)with(
     'connector' = 'kafka',
     'topic' = 'tbl_order_detail',
     'properties.bootstrap.servers' = 'localhost:9092',
     'properties.group.id' = 'testGroup',
     'scan.startup.mode' = 'latest-offset',
     'format' = 'json',
     'json.fail-on-missing-field' = 'false',
     'json.ignore-parse-errors' = 'true'
 );

-- 源生产者

kafka-topics.sh -delete --zookeeper localhost:2181 --topic tbl_order_detail
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic tbl_order_detail
kafka-console-producer.sh --broker-list localhost:9092 --topic tbl_order_detail
kafka-console-consumer.sh --zookeeper localhost:2181 --topic tbl_order_detail --from-beginning

select

    order_book_id                                                                                         as     order_book_id,
    order_id                                                                                              as     order_id,
    book_id                                                                                               as     book_id,
    book_number                                                                                           as     book_number,
    original_price                                                                                        as     original_price,
    actual_price                                                                                          as     actual_price,
    discount_price                                                                                        as     discount_price,
    create_time                                                                                           as     create_time,
    update_time                                                                                           as     update_time,
    dim_product_with_versions(concat('dim_book:',cast(book_id as string)),cast(update_time as bigint))    as dim_product

from tbl_order_detail t1
;


select

    order_book_id                                                                                                                as     order_book_id,
    order_id                                                                                                                     as     order_id,
    book_id                                                                                                                      as     book_id,
    book_number                                                                                                                  as     book_number,
    original_price                                                                                                               as     original_price,
    actual_price                                                                                                                 as     actual_price,
    discount_price                                                                                                               as     discount_price,
    create_time                                                                                                                  as     create_time,
    update_time                                                                                                                  as     update_time,
--    dim_product_with_versions(concat('dim_book:',cast(book_id as string)),cast(update_time as bigint))                           as dim_product,
--    json_value(dim_product_with_versions(concat('dim_book:',cast(book_id as string)),cast(update_time as bigint)),'$.book_name') as book_name,
    json_value(dim_product_with_versions(concat('dim_book:',cast(book_id as string)),cast(update_time as bigint)),'$.price')     as book_price

from tbl_order_detail t1
;


select

    order_book_id,
    order_id,
    book_id,
    json_value(dim_product,'$.price') as price,
    json_value(dim_product,'$.book_name') as book_name

from (
    select

        order_book_id                                                                                      as order_book_id,
        order_id                                                                                           as order_id,
        book_id                                                                                            as book_id,
        book_number                                                                                        as book_number,
        original_price                                                                                     as original_price,
        actual_price                                                                                       as actual_price,
        discount_price                                                                                     as discount_price,
        create_time                                                                                        as create_time,
        update_time                                                                                        as update_time,
        dim_product_with_versions(concat('dim_book:',cast(book_id as string)),cast(update_time as bigint)) as dim_product

    from tbl_order_detail
) tmp
where json_value(dim_product,'$.price') > 5
;





select

    order_book_id                                                                                                                as order_book_id,
    order_id                                                                                                                     as order_id,
    book_id                                                                                                                      as book_id,
    json_value(dim_product_with_versions(concat('dim_book:',cast(book_id as string)),cast(update_time as bigint)),'$.price')     as price,
    json_value(dim_product_with_versions(concat('dim_book:',cast(book_id as string)),cast(update_time as bigint)),'$.book_name') as book_name

from tbl_order_detail
where json_value(dim_product_with_versions(concat('dim_book:',cast(book_id as string)),cast(update_time as bigint)),'$.price') > 5
;




select

    order_book_id,
    order_id,
    book_id,
    json_value(product,'$.price')     as price,
    json_value(product,'$.book_name') as book_name

from (
    select

        order_book_id   as order_book_id,
        order_id        as order_id,
        book_id         as book_id,
        book_number     as book_number,
        original_price  as original_price,
        actual_price    as actual_price,
        discount_price  as discount_price,
        create_time     as create_time,
        update_time     as update_time,
        product         as product

    from tbl_order_detail
    left join lateral table(pass_through(dim_product_with_versions(concat('dim_book:',cast(book_id as string)),cast(update_time as bigint)))) t(product) on true
) tmp
where json_value(product,'$.price') > 5
;








create temporary table tmp as
    select

        order_book_id                                                                                      as order_book_id,
        order_id                                                                                           as order_id,
        book_id                                                                                            as book_id,
        book_number                                                                                        as book_number,
        original_price                                                                                     as original_price,
        actual_price                                                                                       as actual_price,
        discount_price                                                                                     as discount_price,
        create_time                                                                                        as create_time,
        update_time                                                                                        as update_time,
        dim_product_with_versions(concat('dim_book:',cast(book_id as string)),cast(update_time as bigint)) as dim_product

    from tbl_order_detail
;


select

    order_book_id,
    order_id,
    book_id,
    json_value(dim_product,'$.price') as price,
    json_value(dim_product,'$.book_name') as book_name

from tmp
where json_value(dim_product,'$.price') > 5
;


{"book_id":"2","update_time":1714895519000,"original_price":"63.81","create_time":"2024-05-05 14:52:07","actual_price":"63.81","discount_price":"0.0","book_number":"3","order_id":"1","order_book_id":"1"}


select

    order_book_id,
    order_id,
    book_id,
    json_value(dim_product,'$.price') as price,
    json_value(dim_product,'$.book_name') as book_name,

    substr(update_time1,1,10) as substr1,
    substr(update_time1,1,13) as substr2

from (
    select

        order_book_id                                                                                      as order_book_id,
        order_id                                                                                           as order_id,
        book_id                                                                                            as book_id,
        book_number                                                                                        as book_number,
        original_price                                                                                     as original_price,
        actual_price                                                                                       as actual_price,
        discount_price                                                                                     as discount_price,
        create_time                                                                                        as create_time,
        update_time                                                                                        as update_time,
        from_unixtime_udf(update_time,'yyyy-MM-dd HH:mm:ss.SSS')                                           as update_time1,
        dim_product_with_versions(concat('dim_book:',cast(book_id as string)),cast(update_time as bigint)) as dim_product

    from tbl_order_detail
) tmp
where json_value(dim_product,'$.price') > 5
;


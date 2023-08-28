# mock-datasource

这个项目用来进行实时数据学习使用，其可以自动生成《网上书店》交易数据流，其中包括：
交易用户数据、
网上书店数据、
交易图书数据、
订单数据
通过不同的配置可以自动生成顺序的交易数据流，也可以生成乱序的交易数据流。


## 项目涉及原始数据结构
1 tbl_book_shop:
```sql
-- mysql 
create table tbl_book_shop(
    shop_id        int          comment '书店ID',
    shop_name      varchar(128) comment '书店名称',
    shop_address   varchar(128) comment '书店地址',
    level_id       int          comment '书店等级',
    level_name     varchar(32)  comment '书店等级名称',
    city_id        int          comment '城市ID',
    city_name      varchar(64)  comment '城市名称',
    open_date      varchar(32)  comment '开店日期',
    brand_id       int          comment '品牌ID',
    brand_name     varchar(128) comment '品牌名称',
    manager_id     int          comment '经理ID',
    manager_name   varchar(128) comment '经理名称',
    create_time    varchar(32)  comment '创建时间',
    primary key(shop_id)
);

-- hive ODS 
create table tbl_book_shop(
    shop_id        int    comment '书店ID',
    shop_name      string comment '书店名称',
    shop_address   string comment '书店地址',
    level_id       int    comment '书店等级',
    level_name     string comment '书店等级名称',
    city_id        int    comment '城市ID',
    city_name      string comment '城市名称',
    open_date      string comment '开店日期',
    brand_id       int    comment '品牌ID',
    brand_name     string comment '品牌名称',
    manager_id     int    comment '经理ID',
    manager_name   string comment '经理名称',
    create_time    string comment '创建时间'
);
```

2 tbl_book:
```sql
-- mysql 
create table tbl_book(
    book_id        int            comment '图书ID',
    book_name      varchar(128)   comment '图书名称',
    price          double         comment '图书售价',
    category_id    int            comment '品类ID',
    category_name  varchar(128)   comment '品类名称',
    author         varchar(128)   comment '作者',
    publisher      varchar(128)   comment '出版社',
    publisher_date varchar(32)    comment '出版日期',
    create_time    varchar(32)    comment '创建时间',
    primary key(book_id)
);

-- hive ODS 
create table tbl_book(
    book_id        int      comment '图书ID',
    book_name      string   comment '图书名称',
    price          double   comment '图书售价',
    category_id    int      comment '品类ID',
    category_name  string   comment '品类名称',
    author         string   comment '作者',
    publisher      string   comment '出版社',
    publisher_date string   comment '出版日期',
    create_time    string   comment '创建时间' 
);
```

3 tbl_user:
```sql
-- mysql 
create table tbl_user(
    user_id           int            comment '用户ID',
    user_name         varchar(128)   comment '用户名称',
    sex               int            comment '性别',
    account           varchar(64)    comment '账号',
    nick_name         varchar(128)   comment '昵称',
    register_time     varchar(32)    comment '注册时间',
    city_id           int            comment '城市ID',
    city_name         varchar(64)    comment '城市名称',
    crowd_type        varchar(64)    comment '人群类型',
    primary key(user_id)
);

-- hive ODS 
create table tbl_user(
    user_id           int      comment '用户ID',
    user_name         string   comment '用户名称',
    sex               int      comment '性别',
    account           string   comment '账号',
    nick_name         string   comment '昵称',
    register_time     string   comment '注册时间',
    city_id           int      comment '城市ID',
    city_name         string   comment '城市名称',
    crowd_type        string   comment '人群类型' 
);
```

4 tbl_order:
```sql
-- mysql 
create table tbl_order(
    order_id            int         comment '订单ID',
    shop_id             int         comment '书店ID',
    user_id             int         comment '用户ID',
    original_price      double      comment '原始交易额',
    actual_price        double      comment '实付交易额',
    discount_price      double      comment '折扣金额',
    order_status        int         comment '订单状态: 1-提单 2-支付 3-配送 4-完单 5-取消',
    create_time         varchar(32) comment '下单时间',
    primary key(order_id)
);

-- hive ODS 
create table tbl_order(
    order_id            int       comment '订单ID',
    shop_id             int       comment '书店ID',
    user_id             int       comment '用户ID',
    original_price      double    comment '原始交易额',
    actual_price        double    comment '实付交易额',
    discount_price      double    comment '折扣金额',
    order_status        int       comment '订单状态: 1-提单 2-支付 3-配送 4-完单 5-取消',
    create_time         string    comment '下单时间'
);
```

5 tbl_order_detail:
```sql
-- mysql 
create table tbl_order_detail(
    order_book_id           int         comment '订单明细ID',
    order_id                int         comment '订单ID',
    book_id                 int         comment '图书ID',
    book_number             int         comment '图书下单数量',
    original_price          double      comment '原始交易额',
    actual_price            double      comment '实付交易额',
    discount_price          double      comment '折扣金额',
    create_time             varchar(32) comment '下单时间',
    primary key(order_book_id)
);

-- hive ODS 
create table tbl_order_detail(
    order_book_id           int      comment '订单明细ID',
    order_id                int      comment '订单ID',
    book_id                 int      comment '图书ID',
    book_number             int      comment '图书下单数量',
    original_price          double   comment '原始交易额',
    actual_price            double   comment '实付交易额',
    discount_price          double   comment '折扣金额',
    create_time             string   comment '下单时间'    
);
```

6 tbl_order_payment:
```sql
-- mysql 
create table tbl_order_payment(
    pay_id              int         comment '支付ID',
    order_id            int         comment '订单ID',
    payment_amount      double      comment '支付金额',
    payment_status      int         comment '支付状态: 1-支付成功 2-支付失败',
    create_time         varchar(32) comment '支付时间',
    primary key(pay_id)
);

-- hive ODS 
create table tbl_order_payment(
    pay_id              int       comment '支付ID',
    order_id            int       comment '订单ID',
    payment_amount      double    comment '支付金额',
    payment_status      int       comment '支付状态: 0-未支付 1-支付成功 2-支付失败',
    create_time         string    comment '支付时间'
);
```


## 配置文件[mock.properties]
```properties
# kafka config
broker.list = localhost:9092
user.topics = user_topic
book.topics = book_topic
shop.topics = shop_topic
order.topics = order_topic
order.book.topics = order_book_topic
order.payment.topics = order_payment_topic
```

## 数据观测
启动项目后可以通过如下命令观察各个数据源输出情况：
```
kafka-console-consumer.sh --zookeeper localhost:2181 --topic user_topic --from-beginning
kafka-console-consumer.sh --zookeeper localhost:2181 --topic book_topic --from-beginning
kafka-console-consumer.sh --zookeeper localhost:2181 --topic shop_topic --from-beginning
kafka-console-consumer.sh --zookeeper localhost:2181 --topic order_topic --from-beginning
kafka-console-consumer.sh --zookeeper localhost:2181 --topic order_book_topic --from-beginning
kafka-console-consumer.sh --zookeeper localhost:2181 --topic order_payment_topic --from-beginning
```

## 启动方法
```java
public class Test {
    /**
     * orderedFlag     : 是否乱序发送，true表示按生产顺序发出数据；false表示乱序发出数据.
     * sleepTime       : 发送数据时间间隔，单位毫秒
     * unOrderedNum    : 最大乱序数量，表示近unOrderedNum个数据可能存在乱序，最大乱序时间等于unOrderedNum*sleepTime
     */
    public void unitTest() {
        MockDataUtils.mockOrderStreamData(false, 1000, 3);
    }
}
```

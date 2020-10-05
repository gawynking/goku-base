
# Join with a Temporal Table Function 

A join with a temporal table function joins an append-only table (left input/probe side) with a temporal table (right input/build side), i.e., a table that changes over time and tracks its changes. 
Please check the corresponding page for more information about temporal tables.

The following example shows an append-only table Orders that should be joined with the continuously changing currency rates table RatesHistory.

Orders is an append-only table that represents payments for the given amount and the given currency. For example at 10:15 there was an order for an amount of 2 Euro.


具有临时表功能的联接将仅附加表（左侧输入/探针侧）与临时表（右侧输入/构建侧）联接，即随时间变化并跟踪其变化的表。请检查相应的页面以获取有关时态表的更多信息。

以下示例显示了仅附加表Orders，该表应与不断变化的货币汇率表结合在一起RatesHistory。

Orders是一个仅附加表，代表给定amount和给定的付款currency。例如，在处10:15有一笔金额为的订单2 Euro。

```text
SELECT * FROM Orders;

rowtime amount currency
======= ====== =========
10:15        2 Euro
10:30        1 US Dollar
10:32       50 Yen
10:52        3 Euro
11:04        5 US Dollar
```

RatesHistory represents an ever changing append-only table of currency exchange rates with respect to Yen (which has a rate of 1). 
For example, the exchange rate for the period from 09:00 to 10:45 of Euro to Yen was 114. From 10:45 to 11:15 it was 116. 

```text
SELECT * FROM RatesHistory;

rowtime currency   rate
======= ======== ======
09:00   US Dollar   102
09:00   Euro        114
09:00   Yen           1
10:45   Euro        116
11:15   Euro        119
11:49   Pounds      108
```

Given that we would like to calculate the amount of all Orders converted to a common currency (Yen).

For example, we would like to convert the following order using the appropriate conversion rate for the given rowtime (114).

```text
rowtime amount currency
======= ====== =========
10:15        2 Euro
```

Without using the concept of temporal tables, one would need to write a query like:

```text
SELECT
  SUM(o.amount * r.rate) AS amount
FROM Orders AS o,
  RatesHistory AS r
WHERE r.currency = o.currency
AND r.rowtime = (
  SELECT MAX(rowtime)
  FROM RatesHistory AS r2
  WHERE r2.currency = o.currency
  AND r2.rowtime <= o.rowtime);
```
With the help of a temporal table function Rates over RatesHistory, we can express such a query in SQL as:

```text
SELECT
  o.amount * r.rate AS amount
FROM
  Orders AS o,
  LATERAL TABLE (Rates(o.rowtime)) AS r
WHERE r.currency = o.currency
```

Each record from the probe side will be joined with the version of the build side table at the time of the correlated time attribute of the probe side record. 
In order to support updates (overwrites) of previous values on the build side table, the table must define a primary key.

来自探测端的每个记录将与探测端时间属性匹配的构建端表的版本关联。为了支持生成侧表以前值的更新（覆盖），该表必须定义一个主键。

In our example, each record from Orders will be joined with the version of Rates at time o.rowtime. The currency field has been defined as the primary key of Rates before and is used to connect both tables in our example. 
If the query were using a processing-time notion, a newly appended order would always be joined with the most recent version of Rates when executing the operation.

在我们的示例中，来自Order中的每个记录都将与 Rates 在o.rowtime匹配的版本join到一起。该currency字段已定义为Rates 的主键，并且在我们的示例中用于连接两个表。
如果查询使用的是处理时间概念，新添加的订单将始终与Rates的最新版本的join在一起。

In contrast to regular joins, this means that if there is a new record on the build side, 
it will not affect the previous results of the join. This again allows Flink to limit the number of elements that must be kept in the state.

与regular joins相反，这意味着，如果在构建端有新记录，则不会影响先前的join结果。这再次允许Flink限制必须保留在状态中的元素数量。

Compared to time-windowed joins, temporal table joins do not define a time window within which bounds the records will be joined. 
Records from the probe side are always joined with the build side’s version at the time specified by the time attribute. 
Thus, records on the build side might be arbitrarily old. As time passes, the previous and no longer needed versions of the record (for the given primary key) will be removed from the state.

与time-windowed join相比，temporal table joins未定义时间窗口。探测端的记录总是与指定的时间匹配的构建端的版本连接在一起。
因此，构建端的记录可能是任意旧的。随着时间的流逝，该记录的先前版本和不再需要的版本（对于给定的主键）将从状态中删除。

Such behaviour makes a temporal table join a good candidate to express stream enrichment in relational terms.

这种行为使temporal table join成为一个很好的选择，可以用来表示流的富集在关系语义中。

## Usage 

After defining temporal table function, we can start using it. Temporal table functions can be used in the same way as normal table functions would be used.

The following code snippet solves our motivating problem of converting currencies from the Orders table:

在temporal table function之后，我们就可以开始使用它。可以使用与普通表函数相同的方式来使用Temporal table functions。

以下代码段解决了我们从Orders表中转换货币的动机问题：

```text
SELECT
  SUM(o_amount * r_rate) AS amount
FROM
  Orders,
  LATERAL TABLE (Rates(o_proctime))
WHERE
  r_currency = o_currency
```

```text
Table result = orders
    .joinLateral("rates(o_proctime)", "o_currency = r_currency")
    .select("(o_amount * r_rate).sum as amount");
```

Note: State retention defined in a query configuration is not yet implemented for temporal joins. 
This means that the required state to compute the query result might grow infinitely depending on the number of distinct primary keys for the history table.

注意：在查询配置中定义的状态保留尚未针对temporal joins实现。这意味着计算查询结果所需的状态可能会无限增长，具体取决于历史记录表的不同主键数量。


## Processing-time Temporal Joins 

With a processing-time time attribute, it is impossible to pass past time attributes as an argument to the temporal table function. By definition, it is always the current timestamp. 
Thus, invocations of a processing-time temporal table function will always return the latest known versions of the underlying table and any updates in the underlying history table will also immediately 
overwrite the current values.

使用处理时间属性，不可能将过去的时间属性作为参数传递给时态表函数。根据定义，它始终是当前时间戳。因此，处理时间时态表函数的调用将始终返回基础表的最新已知版本，并且基础历史表中的任何更新也将立即覆盖当前值。

Only the latest versions (with respect to the defined primary key) of the build side records are kept in the state. Updates of the build side will have no effect on previously emitted join results.

只有构建端记录的最新版本（相对于已定义的主键）保留在该状态中。构建端的更新将不会影响先前发出的关联结果。

One can think about a processing-time temporal join as a simple HashMap<K, V> that stores all of the records from the build side. When a new record from the build side has the same key as some previous record, 
the old value is just simply overwritten. Every record from the probe side is always evaluated against the most recent/current state of the HashMap.

可以将处理时间时间连接视为HashMap<K, V>存储构建端的所有记录的简单连接。当构建端的新记录具有与先前记录相同的密钥时，旧值只是被覆盖。来自探测器侧的每条记录始终根据最新/当前状态进行评估HashMap。


## Event-time Temporal Joins 

With an event-time time attribute (i.e., a rowtime attribute), it is possible to pass past time attributes to the temporal table function. This allows for joining the two tables at a common point in time.

利用事件时间属性（即行时属性），可以将过去的时间属性传递给时态表函数。这允许在共同的时间点关联两个表。

Compared to processing-time temporal joins, the temporal table does not only keep the latest version (with respect to the defined primary key) of the build side records in the state but stores all versions 
(identified by time) since the last watermark.

与处理时间时间连接相比，时态表不仅保持状态中的构建侧记录的最新版本（相对于定义的主键），而且存储自上一个水印以来的所有版本（由时间标识）。

For example, an incoming row with an event-time timestamp of 12:30:00 that is appended to the probe side table is joined with the version of the build side table at time 12:30:00 according to the concept of temporal tables. 
Thus, the incoming row is only joined with rows that have a timestamp lower or equal to 12:30:00 with applied updates according to the primary key until this point in time.

例如，根据时态表的概念，将事件时间戳为12:30:00的传入行附加到探测侧表并在12:30:00与构建侧表的版本关联。因此，传入行只与时间戳小于或等于12:30:00的行关联，并根据主键应用更新，直到此时为止。

By definition of event time, watermarks allow the join operation to move forward in time and discard versions of the build table that are no longer necessary because no incoming row with lower or equal timestamp is expected.

根据事件时间的定义，水印允许连接操作及时向前移动，并丢弃构建表的版本，这些版本不再是必需的，因为不期望有时间戳较低或相等的输入行。


# Join with a Temporal -- Table 与Temporal Table连接  

A join with a temporal table joins an arbitrary table (left input/probe side) with a temporal table (right input/build side), i.e., an external dimension table that changes over time. 
Please check the corresponding page for more information about temporal tables.

与temporal table的联接将任意表（左侧输入/探针侧）与temporal table（右侧输入/构建侧）联接，即随时间变化的外部维表。请检查相应的页面以获取有关temporal tables的更多信息。

Attention ： Users can not use arbitrary tables as a temporal table, but need to use a table backed by a LookupableTableSource. A LookupableTableSource can only be used for temporal join as a temporal table. 
See the page for more details about how to define LookupableTableSource.

注意：用户不能将任意表用作temporal table，而需要使用由LookupableTableSource支持的表。A LookupableTableSource只能用于temporal联接作为temporal table。有关如何定义LookupableTableSource的更多详细信息，请参见页面。

The following example shows an Orders stream that should be joined with the continuously changing currency rates table LatestRates.

以下示例显示了Orders流与不断变化的货币汇率表LatestRates join在一起。

LatestRates is a dimension table that is materialized with the latest rate. At time 10:15, 10:30, 10:52, the content of LatestRates looks as follows:

LatestRates是物化了最新汇率的维度表。在时间10:15，10:30，10:52，LatestRates的内容如下所示：

```text
10:15> SELECT * FROM LatestRates;

currency   rate
======== ======
US Dollar   102
Euro        114
Yen           1

10:30> SELECT * FROM LatestRates;

currency   rate
======== ======
US Dollar   102
Euro        114
Yen           1


10:52> SELECT * FROM LatestRates;

currency   rate
======== ======
US Dollar   102
Euro        116     <==== changed from 114 to 116
Yen           1
```

The content of LastestRates at time 10:15 and 10:30 are equal. The Euro rate has changed from 114 to 116 at 10:52.

Orders is an append-only table that represents payments for the given amount and the given currency. For example at 10:15 there was an order for an amount of 2 Euro.

LastestRates在10:15到10:30相等。欧元汇率在10:52从114更改为116。

Orders是一个append-only表，代表给定的付款 amount 和 currency。例如，在处10:15有一笔 amount 为的订单 2 Euro。

```text
SELECT * FROM Orders;

amount currency
====== =========
     2 Euro             <== arrived at time 10:15
     1 US Dollar        <== arrived at time 10:30
     2 Euro             <== arrived at time 10:52
```

Given that we would like to calculate the amount of all Orders converted to a common currency (Yen).

For example, we would like to convert the following orders using the latest rate in LatestRates. The result would be:

假设我们要计算全部Orders转换为通用货币（日元）的金额。

例如，我们要使用LatestRates中的最新汇率转换以下订单。结果将是：

```text
amount currency     rate   amout*rate
====== ========= ======= ============
     2 Euro          114          228    <== arrived at time 10:15
     1 US Dollar     102          102    <== arrived at time 10:30
     2 Euro          116          232    <== arrived at time 10:52
```

With the help of temporal table join, we can express such a query in SQL as: 

借助temporal table联接，我们可以在SQL中将查询表示为：

```text
SELECT
  o.amout, o.currency, r.rate, o.amount * r.rate
FROM
  Orders AS o
  JOIN LatestRates FOR SYSTEM_TIME AS OF o.proctime AS r
  ON r.currency = o.currency
```

Each record from the probe side will be joined with the current version of the build side table. In our example, the query is using the processing-time notion, 
so a newly appended order would always be joined with the most recent version of LatestRates when executing the operation. Note that the result is not deterministic for processing-time.

探针端的每个记录都将与构建端表的当前版本关联。在我们的示例中，查询使用的是处理时间概念，因此LatestRates在执行操作时，新附加的订单将始终与LatestRates最新版本的join在一起。注意，结果对于处理时间不是确定的。

In contrast to regular joins, the previous results of the temporal table join will not be affected despite the changes on the build side. 
Also, the temporal table join operator is very lightweight and does not keep any state.

与regular joins相反，尽管在构建方面进行了更改，但temporal table联接对先前结果将不会受到影响。而且，temporal table联接运算符非常轻巧，并且不保留任何状态。

Compared to time-windowed joins, temporal table joins do not define a time window within which the records will be joined. 
Records from the probe side are always joined with the build side’s latest version at processing time. Thus, records on the build side might be arbitrarily old.

与time-windowed联接相比，temporal table联接没有定义时间窗口。在处理时，总是将来自探测端的记录与构建端的最新版本结合在一起。因此，构建端的记录可能是任意旧的。

Both temporal table function join and temporal table join come from the same motivation but have different SQL syntax and runtime implementations:

temporal table function和temporal table join都来自相同的动机，但是具有不同的SQL语法和运行时实现：

The SQL syntax of the temporal table function join is a join UDTF, while the temporal table join uses the standard temporal table syntax introduced in SQL:2011.

temporal table function join的SQL语法是联接UDTF，而temporal table join使用SQL：2011中引入的标准时态表语法。

The implementation of temporal table function joins actually joins two streams and keeps them in state, 
while temporal table joins just receive the only input stream and look up the external database according to the key in the record.

temporal table function joins的实现实际上联接了两个流并使它们保持状态，而temporal table联接仅接收唯一的输入流并根据记录中的键查找外部数据库。

The temporal table function join is usually used to join a changelog stream, while the temporal table join is usually used to join an external table (i.e. dimension table).

temporal table function 联接通常用于联接变更日志流，而temporal table join通常用于联接外部表（即维表）。

Such behaviour makes a temporal table join a good candidate to express stream enrichment in relational terms.

这种行为使时态表成为一个很好的选择，可以用关系术语来表示流的富集。

In the future, the temporal table join will support the features of temporal table function joins, i.e. support to temporal join a changelog stream.

将来，temporal table join将支持temporal table function joins的功能，即支持temporal联接变更日志流。

### Usage 

The syntax of temporal table join is as follows: 

临时表联接的语法如下： 

```text
SELECT [column_list]
FROM table1 [AS <alias1>]
[LEFT] JOIN table2 FOR SYSTEM_TIME AS OF table1.proctime [AS <alias2>]
ON table1.column-name1 = table2.column-name1
```

Currently, only support INNER JOIN and LEFT JOIN. The FOR SYSTEM_TIME AS OF table1.proctime should be followed after temporal table. proctime is a processing time attribute of table1. 
This means that it takes a snapshot of the temporal table at processing time when joining every record from left table.

For example, after defining temporal table, we can use it as following.

当前，仅支持INNER JOIN和LEFT JOIN。FOR SYSTEM_TIME AS OF table1.proctime应跟随temporal table。proctime是table1的处理时间属性。这意味着在连接左表中的每个记录时，它会在处理时为temporal table快照。

例如，在定义时态表之后，我们可以如下使用它。

```text
SELECT
  SUM(o_amount * r_rate) AS amount
FROM
  Orders
  JOIN LatestRates FOR SYSTEM_TIME AS OF o_proctime
  ON r_currency = o_currency
```

Attention : It is only supported in Blink planner.

Attention : It is only supported in SQL, and not supported in Table API yet.

Attention : Flink does not support event time temporal table joins currently.

注意 ： 仅在Blink planner程序中受支持。

注意 ： 仅在SQL中支持，而在Table API中尚不支持。

注意 ： Flink当前不支持事件时间时态表联接。


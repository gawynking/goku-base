
# Temporal Tables ： 时态表 - 对比 拉链表 概念 来理解 

Temporal Tables represent a concept of a (parameterized) view on a changing table that returns the content of a table at a specific point in time.

The changing table can either be a changing history table which tracks the changes (e.g. database changelogs) or a changing dimension table which materializes the changes (e.g. database tables).

For the changing history table, Flink can keep track of the changes and allows for accessing the content of the table at a certain point in time within a query. 
In Flink, this kind of table is represented by a Temporal Table Function.

For the changing dimension table, Flink allows for accessing the content of the table at processing time within a query. In Flink, this kind of table is represented by a Temporal Table.


Temporal Table表示表的变更（参数化）视图的概念，该视图在特定时间点返回表在当时的内容。

Temporal Table可以用来跟踪表的历史快照（例如，数据库更改日志），也可以用来跟踪维表变化（例如，数据库表）。

对于变化的历史记录表，Flink可以跟踪更改，并允许在查询访问表在特定时间点的内容。在Flink中，这种表由Temporal Table Function表示。

对于变化的维表，Flink允许在查询中使用处理时间访问表的内容。在Flink中，这种表由Temporal Table表示。


## Motivation（背景）

### Correlate with a changing history table （与变化的历史记录表相关）

Let’s assume that we have the following table RatesHistory.

假设我们有下表 RatesHistory。 

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

RatesHistory represents an ever growing append-only table of currency exchange rates with respect to Yen (which has a rate of 1). 
For example, the exchange rate for the period from 09:00 to 10:45 of Euro to Yen was 114. From 10:45 to 11:15 it was 116.

Given that we would like to output all current rates at the time 10:58, we would need the following SQL query to compute a result table:

RatesHistory代表一个不断以append-only方式增长的日元货币汇率表（汇率为1）。例如，从09:00到10:45期间Euro到Yen的汇率是114。从10:45到11:15是116。

假如我们希望输出所有在10:58时刻的汇率，我们需要以下SQL查询来计算结果表：

```text
SELECT *
FROM RatesHistory AS r
WHERE r.rowtime = (
  SELECT MAX(rowtime)
  FROM RatesHistory AS r2
  WHERE r2.currency = r.currency
  AND r2.rowtime <= time '10:58');
```

The correlated subquery determines the maximum time for the corresponding currency that is lower or equal than the desired time. The outer query lists the rates that have a maximum timestamp.

The following table shows the result of such a computation. In our example, the update to Euro at 10:45 is taken into account, 
however, the update to Euro at 11:15 and the new entry of Pounds are not considered in the table’s version at time 10:58.

相关子查询确定对应货币的最大时间小于或等于所需时间。外部查询列出具有最大时间戳的速率。

下表显示了这种计算的结果。在我们的示例中，考虑了在10:45对Euro的更新，但是，在11:15对表Euro的更新不在本次版本中。

```text
rowtime currency   rate
======= ======== ======
09:00   US Dollar   102
09:00   Yen           1
10:45   Euro        116
```

The concept of Temporal Tables aims to simplify such queries, speed up their execution, and reduce Flink’s state usage. 
A Temporal Table is a parameterized view on an append-only table that interprets the rows of the append-only table as the changelog of a table and provides the version of that table at a specific point in time. 
Interpreting the append-only table as a changelog requires the specification of a primary key attribute and a timestamp attribute. 
The primary key determines which rows are overwritten and the timestamp determines the time during which a row is valid.

In the above example currency would be a primary key for RatesHistory table and rowtime would be the timestamp attribute.

In Flink, this is represented by a Temporal Table Function.

Temporal Table的概念旨在简化此类查询，加快其执行速度并减少Flink的状态使用。
一个Temporal Table是一个append-only模式表，包含append-only表变更记录并且可以提供在特定时间点上的数据版本信息。
append-only表获取特定的变更日志版本数据，需要同时指定主键和时间戳属性。
主键确定哪些行将被覆盖，时间戳确定哪一行的时间有效。

在上面的示例中，汇率将通过RatesHistory表的主键和rowtime timestamp属性确定。

在Flink中，这由Temporal Table Function表示。

### Correlate with a changing dimension table （与维度变化相关）

On the other hand, some use cases require to join a changing dimension table which is an external database table.

Let’s assume that LatestRates is a table (e.g. stored in) which is materialized with the latest rate. The LatestRates is the materialized history RatesHistory. Then the content of LatestRates table at time 10:58 will be:

另一方面，某些用例需要连接变化的维表，该表是外部数据库表。

假设LatestRates是一个物化了最新汇率的表（例如，存储在其中）。LatestRates是物化的历史数据RatesHistory。那么在10:58时刻LatestRates表的内容是：

```text
10:58> SELECT * FROM LatestRates;
currency   rate
======== ======
US Dollar   102
Yen           1
Euro        116
```

The content of LatestRates table at time 12:00 will be: 

那么在12:00时刻LatestRates表的内容是：

```text
12:00> SELECT * FROM LatestRates;
currency   rate
======== ======
US Dollar   102
Yen           1
Euro        119
Pounds      108
```

In Flink, this is represented by a Temporal Table. 

在flink中，这种场景表示为Temporal Table。

## Temporal Table Function 

In order to access the data in a temporal table, one must pass a time attribute that determines the version of the table that will be returned. Flink uses the SQL syntax of table functions to provide a way to express it.

Once defined, a Temporal Table Function takes a single time argument timeAttribute and returns a set of rows. 
This set contains the latest versions of the rows for all of the existing primary keys with respect to the given time attribute.

Assuming that we defined a temporal table function Rates(timeAttribute) based on RatesHistory table, we could query such a function in the following way:

为了访问temporal table中的数据，必须传递一个时间属性，该属性确定将要返回的表的版本。Flink使用表函数的SQL语法提供一种表达它的方法。

定义后，Temporal Table Function将使用单个时间参数（timeAttribute）并返回数据集。该集合包含相对于给定时间属性的所有现有主键的行的最新版本。

假设我们定义了一个基于RatesHistory表的temporal table function Rates(timeAttribute)，则可以通过以下方式查询该函数：

```text
SELECT * FROM Rates('10:15');

rowtime currency   rate
======= ======== ======
09:00   US Dollar   102
09:00   Euro        114
09:00   Yen           1

SELECT * FROM Rates('11:00');

rowtime currency   rate
======= ======== ======
09:00   US Dollar   102
10:45   Euro        116
09:00   Yen           1
```

笔记： 表函数 返回一张表 ，一次一张flink表可以直接和表函数结果进行join 

Each query to Rates(timeAttribute) would return the state of the Rates for the given timeAttribute.

Note: Currently, Flink doesn’t support directly querying the temporal table functions with a constant time attribute parameter. 
At the moment, temporal table functions can only be used in joins. The example above was used to provide an intuition about what the function Rates(timeAttribute) returns.

See also the page about joins for continuous queries for more information about how to join with a temporal table.

每个查询Rates(timeAttribute)都会返回给定timeAttribute时的状态数据。

注意：当前，Flink不支持使用常量时间属性参数直接查询temporal table functions。
目前，temporal table functions只能在join中使用。上面的示例只是为了提供有关函数Rates(timeAttribute)返回内容的直观信息(并不代表可以直接这么用)。

另请参阅有关用于连续查询的联接的页面，以获取有关如何与temporal table join的更多信息。

### Defining Temporal Table Function 

The following code snippet illustrates how to create a temporal table function from an append-only table. 

以下代码段说明了如何从append-only表中创建temporal table function。 

```text
import org.apache.flink.table.functions.TemporalTableFunction;
(...)

// Get the stream and table environments.
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

// Provide a static data set of the rates history table.
List<Tuple2<String, Long>> ratesHistoryData = new ArrayList<>();
ratesHistoryData.add(Tuple2.of("US Dollar", 102L));
ratesHistoryData.add(Tuple2.of("Euro", 114L));
ratesHistoryData.add(Tuple2.of("Yen", 1L));
ratesHistoryData.add(Tuple2.of("Euro", 116L));
ratesHistoryData.add(Tuple2.of("Euro", 119L));

// Create and register an example table using above data set.
// In the real setup, you should replace this with your own table.
DataStream<Tuple2<String, Long>> ratesHistoryStream = env.fromCollection(ratesHistoryData);
Table ratesHistory = tEnv.fromDataStream(ratesHistoryStream, "r_currency, r_rate, r_proctime.proctime");

tEnv.createTemporaryView("RatesHistory", ratesHistory);

// Create and register a temporal table function.
// Define "r_proctime" as the time attribute and "r_currency" as the primary key.
TemporalTableFunction rates = ratesHistory.createTemporalTableFunction("r_proctime", "r_currency"); // <==== (1)
tEnv.registerFunction("Rates", rates);                                                              // <==== (2)
```

Line (1) creates a rates temporal table function, which allows us to use the function rates in the Table API.

Line (2) registers this function under the name Rates in our table environment, which allows us to use the Rates function in SQL. 

Line (1) : 创建了rates temporal table function，使我们可以在Table API中使用rates函数。

Line (2) : 在我们的表环境中以名称Rates注册该函数，这使我们可以在SQL中使用Rates函数。


## Temporal Table 

Attention ： This is only supported in Blink planner.

In order to access data in temporal table, currently one must define a TableSource with LookupableTableSource. Flink uses the SQL syntax of FOR SYSTEM_TIME AS OF to query temporal table, which is proposed in SQL:2011.

Assuming that we defined a temporal table called LatestRates, we can query such a table in the following way: 

注意：仅Blink planner支持此功能。

为了访问temporal table数据，目前必须定义一个TableSource与LookupableTableSource。Flink使用SQL语法FOR SYSTEM_TIME AS OF查询temporal table，这种写法仅被支持在SQL：2011。

假设我们定义了一个名为LatestRates temporal table，我们可以通过以下方式查询此类表：

```text
SELECT * FROM LatestRates FOR SYSTEM_TIME AS OF TIME '10:15';

currency   rate
======== ======
US Dollar   102
Euro        114
Yen           1

SELECT * FROM LatestRates FOR SYSTEM_TIME AS OF TIME '11:00';

currency   rate
======== ======
US Dollar   102
Euro        116
Yen           1
```

Note: Currently, Flink doesn’t support directly querying the temporal table with a constant time. At the moment, temporal table can only be used in joins. 
The example above is used to provide an intuition about what the temporal table LatestRates returns.

See also the page about joins for continuous queries for more information about how to join with a temporal table.

注意：当前，Flink不支持以常量时间方式直接查询temporal table。目前，temporal table只能在join中使用。上面的示例仅用于直观展示temporal table LatestRates返回内容。

另请参阅有关用于joins for continuous queries页面，以获取有关如何与时态表联接的更多信息。

### Defining Temporal Table 

```text
// Get the stream and table environments.
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

// Create an HBaseTableSource as a temporal table which implements LookableTableSource
// In the real setup, you should replace this with your own table.
HBaseTableSource rates = new HBaseTableSource(conf, "Rates");
rates.setRowKey("currency", String.class);   // currency as the primary key
rates.addColumn("fam1", "rate", Double.class);

// register the temporal table into environment, then we can query it in sql
tEnv.registerTableSource("Rates", rates);
```
See also the page about how to define LookupableTableSource. 

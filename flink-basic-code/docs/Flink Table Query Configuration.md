
# Query Configuration 

Table API and SQL queries have the same semantics regardless whether their input is bounded batch input or unbounded stream input. In many cases, continuous queries on streaming input are capable of computing accurate 
results that are identical to offline computed results. However, this is not possible in general case because continuous queries have to restrict the size of the state they are maintaining in order to avoid to run out 
of storage and to be able to process unbounded streaming data over a long period of time. As a result, a continuous query might only be able to provide approximated results depending on the characteristics of the input 
data and the query itself.

Flink’s Table API and SQL interface provide parameters to tune the accuracy and resource consumption of continuous queries. The parameters are specified via a TableConfig object. The TableConfig can be obtained 
from the TableEnvironment and is passed back when a Table is translated, i.e., when it is transformed into a DataStream or emitted via a TableSink.

Table API和SQL查询具有相同的语义，无论它们的输入是有界批处理输入还是无界流输入。在许多情况下，对流输入的连续查询能够计算出与脱机计算结果相同的精确结果。但是，这在一般情况下是不可能的，因为连续查询必须限制它们所维护的状态的大小，
以便避免耗尽存储并能够在很长一段时间内处理无限制的流数据。因此，连续查询可能只能根据输入数据和查询本身的特性提供近似的结果。

Flink的表API和SQL接口提供参数来优化连续查询的准确性和资源消耗。参数是通过TableConfig对象指定的。TableConfig可以从TableEnvironment中获得，并在转换表时传回，即，当它被转换为DataStream或通过Table Sink发出时。

```text
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// obtain query configuration from TableEnvironment
TableConfig tConfig = tableEnv.getConfig();
// set query parameters
tConfig.setIdleStateRetentionTime(Time.hours(12), Time.hours(24));

// define query
Table result = ...

// create TableSink
TableSink<Row> sink = ...

// register TableSink
tableEnv.registerTableSink(
  "outputTable",               // table name
  new String[]{...},           // field names
  new TypeInformation[]{...},  // field types
  sink);                       // table sink

// emit result Table via a TableSink
result.insertInto("outputTable");

// convert result Table into a DataStream<Row>
DataStream<Row> stream = tableEnv.toAppendStream(result, Row.class);
```

In the following we describe the parameters of the TableConfig and how they affect the accuracy and resource consumption of a query. 

在下文中，我们将描述它们的参数TableConfig以及它们如何影响查询的准确性和资源消耗。

## Idle State Retention Time 

Many queries aggregate or join records on one or more key attributes. When such a query is executed on a stream, the continuous query needs to collect records or maintain partial results per key. 
If the key domain of the input stream is evolving, i.e., the active key values are changing over time, the continuous query accumulates more and more state as more and more distinct keys are observed. However, 
often keys become inactive after some time and their corresponding state becomes stale and useless.

For example the following query computes the number of clicks per session.

许多查询聚合或联接一个或多个关键属性上的记录。当在流上执行此类查询时，连续查询需要收集记录或维护每个键的部分结果。如果输入流的key数量在演进，即，活动键值随时间变化，随着观察到的键越来越多，连续查询的状态也越来越多。
但是，经常在一段时间后密钥变为非活动状态，并且它们的相应状态变得陈旧且无用。

例如，下面的查询计算每个会话的单击次数。

```dbn-sql
SELECT sessionId, COUNT(*) FROM clicks GROUP BY sessionId;
```

The sessionId attribute is used as a grouping key and the continuous query maintains a count for each sessionId it observes. 

The sessionId attribute is evolving over time and sessionId values are only active until the session ends, i.e., for a limited period of time. 

However, the continuous query cannot know about this property of sessionId and expects that every sessionId value can occur at any point of time. 

It maintains a count for each observed sessionId value. Consequently, the total state size of the query is continuously growing as more and more sessionId values are observed.

The Idle State Retention Time parameters define for how long the state of a key is retained without being updated before it is removed. 

For the previous example query, the count of a sessionId would be removed as soon as it has not been updated for the configured period of time.

sessionId属性用作分组键，连续查询为它观察到的每个sessionId维护一个计数。

sessionId属性会随着时间的推移而变化，并且sessionId值只在会话结束之前是活动的，即，在有限的一段时间内。

但是，连续查询不能知道sessionId的这个属性，并且期望每个sessionId值都可以在任何时间点出现。

它为每个观察到的sessionId值维护一个计数。因此，随着观察到越来越多的sessionId值，查询的总状态大小不断增长。

空闲状态保留时间参数定义键的状态保留多长时间，在删除键之前不进行更新。

对于上一个示例查询，只要在配置的一段时间内没有更新sessionId，它的计数就会被删除。

By removing the state of a key, the continuous query completely forgets that it has seen this key before. If a record with a key, whose state has been removed before, is processed, 
the record will be treated as if it was the first record with the respective key. For the example above this means that the count of a sessionId would start again at 0.

There are two parameters to configure the idle state retention time:

The minimum idle state retention time defines how long the state of an inactive key is at least kept before it is removed.
The maximum idle state retention time defines how long the state of an inactive key is at most kept before it is removed.

The parameters are specified as follows:

通过删除键的状态，连续查询完全忘记它以前见过这个键。如果处理具有键的记录(其状态以前已被删除)，则该记录将被视为具有相应键的第一个记录。对于上面的示例，这意味着sessionId的计数将从0开始。

配置空闲状态保留时间有两个参数:

最小空闲状态保留时间 定义了一个非活动键在被删除之前至少保持其状态的时间。 
最大空闲状态保留时间 定义了一个非活动键在被删除之前最多保留多长时间。 

具体参数如下:

```text
TableConfig tConfig = ...

// set idle state retention time: min = 12 hours, max = 24 hours
tConfig.setIdleStateRetentionTime(Time.hours(12), Time.hours(24));
```

Cleaning up state requires additional bookkeeping which becomes less expensive for larger differences of minTime and maxTime. The difference between minTime and maxTime must be at least 5 minutes.

清理状态需要额外记账，由于minTime和maxTime的差异较大，这样做的成本更低。minTime和maxTime之间的差异必须至少为5分钟。


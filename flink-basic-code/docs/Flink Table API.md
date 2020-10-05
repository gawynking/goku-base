
# Table API


参考：
https://ci.apache.org/projects/flink/flink-docs-release-1.10/zh/dev/table/tableApi.html 

https://flink-docs-cn.gitbook.io/project/05-ying-yong-kai-fa/04-table-api-and-sql/table-api 


The Table API is a unified, relational API for stream and batch processing. Table API queries can be run on batch or streaming input without modifications. The Table API is a super set of the SQL language and is specially designed for working with Apache Flink. The Table API is a language-integrated API for Scala and Java. Instead of specifying queries as String values as common with SQL, Table API queries are defined in a language-embedded style in Java or Scala with IDE support like autocompletion and syntax validation.

The Table API shares many concepts and parts of its API with Flink’s SQL integration. Have a look at the Common Concepts & API to learn how to register tables or to create a Table object. The Streaming Concepts pages discuss streaming specific concepts such as dynamic tables and time attributes.

The following examples assume a registered table called Orders with attributes (a, b, c, rowtime). The rowtime field is either a logical time attribute in streaming or a regular timestamp field in batch.

Table API是用于流和批处理的统一关系API。 表API查询可以在批量或流式输入上运行而无需修改。 Table API是SQL语言的超级集合，专门用于Apache Flink。 Table API是Scala和Java的语言集成API。 Table API查询不是像SQL中常见的那样将查询指定为String值，而是在Java或Scala中以嵌入语言的样式定义，具有IDE支持，如自动完成和语法验证。
Table API与Flink的SQL集成共享其API的许多概念和部分。查看Common Concepts＆API以了解如何注册表或创建Table对象。该流概念的网页讨论流如动态表和时间属性，具体的概念。
以下示例假设一个名为Orders的已注册表，其中包含属性（a，b，c，rowtime）。 rowtime字段是流式传输中的逻辑时间属性或批处理中的常规时间戳字段。

## Overview & Examples 


The Table API is available for Scala and Java. The Scala Table API leverages on Scala expressions, 
the Java Table API is based on strings which are parsed and converted into equivalent expressions.

The following example shows the differences between the Scala and Java Table API. The table program is executed in a batch environment. It scans the Orders table, 
groups by field a, and counts the resulting rows per group. The result of the table program is converted into a DataSet of type Row and printed.

Table API可用于Scala和Java。Scala Table API利用Scala表达式，Java Table API基于字符串，这些字符串被解析并转换为等效表达式。

以下示例显示了Scala和Java Table API之间的差异。 表程序在批处理环境中执行。 它按字段扫描Orders表，分组，并计算每组的结果行数。 表程序的结果转换为Row类型的DataSet并打印。

The Java Table API is enabled by importing org.apache.flink.table.api.java.*. 
The following example shows how a Java Table API program is constructed and how expressions are specified as strings.

通过导入org.apache.flink.table.api.java.*来启用Java Table API。 以下示例显示如何构造Java Table API程序以及如何将表达式指定为字符串。



```java
// environment configuration
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

// register Orders table in table environment
// ...

// specify table program
Table orders = tEnv.from("Orders"); // schema (a, b, c, rowtime)

Table counts = orders
        .groupBy("a")
        .select("a, b.count as cnt");

// conversion to DataSet
DataSet<Row> result = tEnv.toDataSet(counts, Row.class);
result.print();
```

The next example shows a more complex Table API program. The program scans again the Orders table. It filters null values, normalizes the field a of type String,
and calculates for each hour and product a the average billing amount b.

下一个示例显示了一个更复杂的Table API程序。 程序再次扫描Orders表。 它过滤空值，规范化String类型的字段a，并计算每小时和产品的平均计费金额b。

```java
// environment configuration
// ...

// specify table program
Table orders = tEnv.from("Orders"); // schema (a, b, c, rowtime)

Table result = orders
        .filter("a.isNotNull && b.isNotNull && c.isNotNull")
        .select("a.lowerCase() as a, b, rowtime")
        .window(Tumble.over("1.hour").on("rowtime").as("hourlyWindow"))
        .groupBy("hourlyWindow, a")
        .select("a, hourlyWindow.end as hour, b.avg as avgBillingAmount");
```

Since the Table API is a unified API for batch and streaming data, both example programs can be executed on batch and streaming inputs without any modification of the table program itself. In both cases, 
the program produces the same results given that streaming records are not late (see Streaming Concepts for details).

由于Table API是批量和流数据的统一API，因此两个示例程序都可以在批处理和流式输入上执行，而无需对表程序本身进行任何修改。
在这两种情况下，程序产生相同的结果，因为流记录不延迟（有关详细信息，请参阅流式概念）。

## Operations

The Table API supports the following operations. Please note that not all operations are available in both batch and streaming yet; they are tagged accordingly.

Table API支持以下操作。请注意，并非所有操作都可用于批处理和流式处理; 他们被相应地标记。

### Scan, Projection, and Filter :

from           : 与SQL查询中的FROM子句类似。
Select         : 与SQL SELECT语句类似。 
as             : 重命名字段。 
Where / Filter : 与SQL WHERE子句类似。 过滤掉未通过过滤谓词的行。 
    Table orders = tableEnv.from("Orders");
    Table result = orders.where("b === 'red'");
        or
    Table orders = tableEnv.from("Orders");
    Table result = orders.filter("a % 2 === 0");

### Column Operations 













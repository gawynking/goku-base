
# Connect to External Systems 

Flink’s Table API & SQL programs can be connected to other external systems for reading and writing both batch and streaming tables. A table source provides access to data which is stored in external systems (such as a database, key-value store, message queue, or file system). A table sink emits a table to an external storage system. Depending on the type of source and sink, they support different formats such as CSV, Parquet, or ORC.

This page describes how to declare built-in table sources and/or table sinks and register them in Flink. After a source or sink has been registered, it can be accessed by Table API & SQL statements.

Attention If you want to implement your own custom table source or sink, have a look at the user-defined sources & sinks page.

Flink的Table API和SQL程序可以连接到其他外部系统，以读取和写入批处理表和流式表。表源提供对存储在外部系统（例如数据库，键值存储，消息队列或文件系统）中的数据的访问。表接收器将表发送到外部存储系统。
根据源和接收器的类型，它们支持不同的格式，例如CSV，Parquet或ORC。

本页介绍如何声明内置表源和/或表接收器，以及如何在Flink中注册它们。注册源或接收器后，可以通过Table API和SQL语句对其进行访问。

注意如果要实现自己的定制表源或接收器，请查看用户定义的源和接收器页面。

## Dependencies 

```text
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-common</artifactId>
            <version>${flink.version}</version>
            <!--<scope>provided</scope>-->
        </dependency>

        <!-- https://mvnrepository.com/artifact/org.apache.flink/flink-connector-kafka -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-kafka_2.12</artifactId>
            <version>${flink.version}</version>
        </dependency>
```
        
## Overview 

Beginning from Flink 1.6, the declaration of a connection to an external system is separated from the actual implementation.

从Flink 1.6开始，与外部系统的连接声明与实际实现分开了。

Connections can be specified either

可以指定连接: 

    programmatically using a Descriptor under org.apache.flink.table.descriptors for Table & SQL API
    or declaratively via YAML configuration files for the SQL Client.
    
    以编程方式使用org.apache.flink.table.descriptors下的描述符，用于表和SQL API.
    或以声明方式通过SQL客户端的YAML配置文件。
    
This allows not only for better unification of APIs and SQL Client but also for better extensibility in case of custom implementations without changing the actual declaration.

这不仅可以更好地统一API和SQL Client，还可以在自定义实现的情况下更好地扩展而不更改实际声明。

Every declaration is similar to a SQL CREATE TABLE statement. One can define the name of the table, the schema of the table, a connector, and a data format upfront for connecting to an external system.

每个声明都类似于SQL CREATE TABLE语句。可以定义表的名称，表的架构，连接器以及用于连接到外部系统的数据格式。

The connector describes the external system that stores the data of a table. Storage systems such as Apacha Kafka or a regular file system can be declared here. 
The connector might already provide a fixed format with fields and schema.

连接器描述了外部系统表明表中的存储数据。可以在此处声明诸如Apacha Kafka之类的存储系统或常规文件系统。连接器可能已经提供了带有字段和架构的固定格式。

Some systems support different data formats. For example, a table that is stored in Kafka or in files can encode its rows with CSV, JSON, or Avro. A database connector might need the table schema here. 
Whether or not a storage system requires the definition of a format, is documented for every connector. Different systems also require different types of formats (e.g., column-oriented formats vs. row-oriented formats). 
The documentation states which format types and connectors are compatible.

一些系统支持不同的数据格式。例如，存储在Kafka或文件中的表可以使用CSV，JSON或Avro对行进行编码。数据库连接器可能需要此处的表架构。每个连接器都记录了存储系统是否需要格式的定义。
不同的系统还需要不同类型的格式（例如，面向列的格式与面向行的格式）。该文档说明了哪些格式类型和连接器兼容。

The table schema defines the schema of a table that is exposed to SQL queries. It describes how a source maps the data format to the table schema and a sink vice versa. 
The schema has access to fields defined by the connector or format. It can use one or more fields for extracting or inserting time attributes. 
If input fields have no deterministic field order, the schema clearly defines column names, their order, and origin.

表架构定义了SQL查询的表的架构。它描述了源如何将数据格式映射到表模式，反之亦然。该模式可以访问由连接器或格式定义的字段。它可以使用一个或多个字段来提取或插入时间属性。如果输入字段没有确定性的字段顺序，则该架构将明确定义列名称，其顺序和来源。

The subsequent sections will cover each definition part (connector, format, and schema) in more detail. The following example shows how to pass them:

后续各节将更详细地介绍每个定义部分（connector，format和schema）。以下示例显示了如何应用它们：

DDL:
```text
tableEnvironment.sqlUpdate(
    "CREATE TABLE MyTable (\n" +
    "  ...    -- declare table schema \n" +
    ") WITH (\n" +
    "  'connector.type' = '...',  -- declare connector specific properties\n" +
    "  ...\n" +
    "  'update-mode' = 'append',  -- declare update mode\n" +
    "  'format.type' = '...',     -- declare format specific properties\n" +
    "  ...\n" +
    ")");
```

java 
```text
tableEnvironment
  .connect(...)
  .withFormat(...)
  .withSchema(...)
  .inAppendMode()
  .createTemporaryTable("MyTable")
```

ymal:
```text
name: MyTable
type: source
update-mode: append
connector: ...
format: ...
schema: ...
```

The table’s type (source, sink, or both) determines how a table is registered. In case of table type both, both a table source and table sink are registered under the same name. 
Logically, this means that we can both read and write to such a table similarly to a table in a regular DBMS.

表格的类型（source，sink或both）确定如何注册表格。对于所有表类型，表源和表接收器都以相同的名称注册。从逻辑上讲，这意味着我们可以像读取常规DBMS中的表一样读取和写入该表。

For streaming queries, an update mode declares how to communicate between a dynamic table and the storage system for continuous queries.

对于流查询，更新模式声明了如何在动态表和存储系统之间进行通信以进行连续查询。

The following code shows a full example of how to connect to Kafka for reading Avro records.

以下代码显示了如何连接到Kafka以读取Avro记录的完整示例。

DDL:
```text
CREATE TABLE MyUserTable (
  -- declare the schema of the table
  `user` BIGINT,
  message STRING,
  ts STRING
) WITH (
  -- declare the external system to connect to
  'connector.type' = 'kafka',
  'connector.version' = '0.10',
  'connector.topic' = 'topic_name',
  'connector.startup-mode' = 'earliest-offset',
  'connector.properties.zookeeper.connect' = 'localhost:2181',
  'connector.properties.bootstrap.servers' = 'localhost:9092',

  -- specify the update-mode for streaming tables
  'update-mode' = 'append',

  -- declare a format for this system
  'format.type' = 'avro',
  'format.avro-schema' = '{
                            "namespace": "org.myorganization",
                            "type": "record",
                            "name": "UserMessage",
                            "fields": [
                                {"name": "ts", "type": "string"},
                                {"name": "user", "type": "long"},
                                {"name": "message", "type": ["string", "null"]}
                            ]
                         }'
)
```

java:
```text
tableEnvironment
  // declare the external system to connect to
  .connect(
    new Kafka()
      .version("0.10")
      .topic("test-input")
      .startFromEarliest()
      .property("zookeeper.connect", "localhost:2181")
      .property("bootstrap.servers", "localhost:9092")
  )

  // declare a format for this system
  .withFormat(
    new Avro()
      .avroSchema(
        "{" +
        "  \"namespace\": \"org.myorganization\"," +
        "  \"type\": \"record\"," +
        "  \"name\": \"UserMessage\"," +
        "    \"fields\": [" +
        "      {\"name\": \"timestamp\", \"type\": \"string\"}," +
        "      {\"name\": \"user\", \"type\": \"long\"}," +
        "      {\"name\": \"message\", \"type\": [\"string\", \"null\"]}" +
        "    ]" +
        "}"
      )
  )

  // declare the schema of the table
  .withSchema(
    new Schema()
      .field("rowtime", DataTypes.TIMESTAMP(3))
        .rowtime(new Rowtime()
          .timestampsFromField("timestamp")
          .watermarksPeriodicBounded(60000)
        )
      .field("user", DataTypes.BIGINT())
      .field("message", DataTypes.STRING())
  )

  // create a table with given name
  .createTemporaryTable("MyUserTable");
```

ymal:
```text
tables:
  - name: MyUserTable      # name the new table
    type: source           # declare if the table should be "source", "sink", or "both"

    # declare the external system to connect to
    connector:
      type: kafka
      version: "0.10"
      topic: test-input
      startup-mode: earliest-offset
      properties:
        zookeeper.connect: localhost:2181
        bootstrap.servers: localhost:9092

    # declare a format for this system
    format:
      type: avro
      avro-schema: >
        {
          "namespace": "org.myorganization",
          "type": "record",
          "name": "UserMessage",
            "fields": [
              {"name": "ts", "type": "string"},
              {"name": "user", "type": "long"},
              {"name": "message", "type": ["string", "null"]}
            ]
        }

    # declare the schema of the table
    schema:
      - name: rowtime
        data-type: TIMESTAMP(3)
        rowtime:
          timestamps:
            type: from-field
            from: ts
          watermarks:
            type: periodic-bounded
            delay: "60000"
      - name: user
        data-type: BIGINT
      - name: message
        data-type: STRING
```

In both ways the desired connection properties are converted into normalized, string-based key-value pairs. 
So-called table factories create configured table sources, table sinks, and corresponding formats from the key-value pairs. 
All table factories that can be found via Java’s Service Provider Interfaces (SPI) are taken into account when searching for exactly-one matching table factory.

两种方式都将所需的连接属性转换为标准化的基于字符串的键值对。所谓的表工厂根据键值对创建已配置的表源，表接收器和相应的格式。搜索精确匹配的一个表工厂时，将考虑通过Java的服务提供商接口（SPI）可以找到的所有表工厂。

If no factory can be found or multiple factories match for the given properties, an exception will be thrown with additional information about considered factories and supported properties.

如果找不到给定属性的工厂或多个工厂匹配，则将引发异常，并提供有关考虑的工厂和支持的属性的其他信息。

## Table Schema 

The table schema defines the names and types of columns similar to the column definitions of a SQL CREATE TABLE statement. 
In addition, one can specify how columns are mapped from and to fields of the format in which the table data is encoded. 
The origin of a field might be important if the name of the column should differ from the input/output format. 
For instance, a column user_name should reference the field $$-user-name from a JSON format. 
Additionally, the schema is needed to map types from an external system to Flink’s representation. In case of a table sink, it ensures that only data with valid schema is written to an external system.

表模式定义列的名称和类型，类似于SQL CREATE TABLE语句的列定义。
此外，可以指定如何将列与表数据编码格式的字段进行映射。如果列名应与输入/输出格式不同，则字段的来源可能很重要。
例如，一列user_name应引用$$-user-name 从JSON格式的字段。此外，需要使用该架构将类型从外部系统映射到Flink的表示形式。如果是表接收器，则可确保仅将具有有效架构的数据写入外部系统。

The following example shows a simple schema without time attributes and one-to-one field mapping of input/output to table columns.

以下示例显示了一个没有时间属性的简单架构，并且输入/输出到表列的一对一字段映射。

```text
.withSchema(
  new Schema()
    .field("MyField1", DataTypes.INT())     // required: specify the fields of the table (in this order)
    .field("MyField2", DataTypes.STRING())
    .field("MyField3", DataTypes.BOOLEAN())
)
```

yaml:
```text
schema:
  - name: MyField1    # required: specify the fields of the table (in this order)
    data-type: INT
  - name: MyField2
    data-type: STRING
  - name: MyField3
    data-type: BOOLEAN
```

For each field, the following properties can be declared in addition to the column’s name and type: 

对于每个字段，除列的名称和类型外，还可以声明以下属性： 

```text
.withSchema(
  new Schema()
    .field("MyField1", DataTypes.TIMESTAMP(3))
      .proctime()      // optional: declares this field as a processing-time attribute
    .field("MyField2", DataTypes.TIMESTAMP(3))
      .rowtime(...)    // optional: declares this field as a event-time attribute
    .field("MyField3", DataTypes.BOOLEAN())
      .from("mf3")     // optional: original field in the input that is referenced/aliased by this field
)
```

```text
schema:
  - name: MyField1
    data-type: TIMESTAMP(3)
    proctime: true    # optional: boolean flag whether this field should be a processing-time attribute
  - name: MyField2
    data-type: TIMESTAMP(3)
    rowtime: ...      # optional: wether this field should be a event-time attribute
  - name: MyField3
    data-type: BOOLEAN
    from: mf3         # optional: original field in the input that is referenced/aliased by this field
```

Time attributes are essential when working with unbounded streaming tables. Therefore both processing-time and event-time (also known as “rowtime”) attributes can be defined as part of the schema.

使用无界流表时，时间属性至关重要。因此，处理时间和事件时间（也称为“行时间”）属性都可以定义为架构的一部分。

For more information about time handling in Flink and especially event-time, we recommend the general event-time section.

有关Flink中时间处理（尤其是事件时间）的更多信息，我们建议使用常规事件时间部分。

### Rowtime Attributes 

In order to control the event-time behavior for tables, Flink provides predefined timestamp extractors and watermark strategies.

为了控制表的事件时间行​​为，Flink提供了预定义的时间戳提取器和水印策略。

The following timestamp extractors are supported:

支持以下时间戳提取器：

```text
// Converts an existing LONG or SQL_TIMESTAMP field in the input into the rowtime attribute.
.rowtime(
  new Rowtime()
    .timestampsFromField("ts_field")    // required: original field name in the input
)

// Converts the assigned timestamps from a DataStream API record into the rowtime attribute
// and thus preserves the assigned timestamps from the source.
// This requires a source that assigns timestamps (e.g., Kafka 0.10+).
.rowtime(
  new Rowtime()
    .timestampsFromSource()
)

// Sets a custom timestamp extractor to be used for the rowtime attribute.
// The extractor must extend `org.apache.flink.table.sources.tsextractors.TimestampExtractor`.
.rowtime(
  new Rowtime()
    .timestampsFromExtractor(...)
)
```

```text
# Converts an existing BIGINT or TIMESTAMP field in the input into the rowtime attribute.
rowtime:
  timestamps:
    type: from-field
    from: "ts_field"                 # required: original field name in the input

# Converts the assigned timestamps from a DataStream API record into the rowtime attribute
# and thus preserves the assigned timestamps from the source.
rowtime:
  timestamps:
    type: from-source
```

The following watermark strategies are supported: 

支持以下水印策略： 

```text
// Sets a watermark strategy for ascending rowtime attributes. Emits a watermark of the maximum
// observed timestamp so far minus 1. Rows that have a timestamp equal to the max timestamp
// are not late.
.rowtime(
  new Rowtime()
    .watermarksPeriodicAscending()
)

// Sets a built-in watermark strategy for rowtime attributes which are out-of-order by a bounded time interval.
// Emits watermarks which are the maximum observed timestamp minus the specified delay.
.rowtime(
  new Rowtime()
    .watermarksPeriodicBounded(2000)    // delay in milliseconds
)

// Sets a built-in watermark strategy which indicates the watermarks should be preserved from the
// underlying DataStream API and thus preserves the assigned watermarks from the source.
.rowtime(
  new Rowtime()
    .watermarksFromSource()
)
```

```text
# Sets a watermark strategy for ascending rowtime attributes. Emits a watermark of the maximum
# observed timestamp so far minus 1. Rows that have a timestamp equal to the max timestamp
# are not late.
rowtime:
  watermarks:
    type: periodic-ascending

# Sets a built-in watermark strategy for rowtime attributes which are out-of-order by a bounded time interval.
# Emits watermarks which are the maximum observed timestamp minus the specified delay.
rowtime:
  watermarks:
    type: periodic-bounded
    delay: ...                # required: delay in milliseconds

# Sets a built-in watermark strategy which indicates the watermarks should be preserved from the
# underlying DataStream API and thus preserves the assigned watermarks from the source.
rowtime:
  watermarks:
    type: from-source
```

Make sure to always declare both timestamps and watermarks. Watermarks are required for triggering time-based operations. 

确保始终声明时间戳和水印。触发基于时间的操作需要水印。

### Type Strings 

Because DataType is only available in a programming language, type strings are supported for being defined in a YAML file. 
The type strings are the same to type declaration in SQL, please see the Data Types page about how to declare a type in SQL.

因为DataType仅在编程语言中可用，所以支持在YAML文件中定义字符串类型。字符串类型与SQL中的类型声明相同，请参见“ 数据类型”页面，了解如何在SQL中声明类型。

### Update Modes

For streaming queries, it is required to declare how to perform the conversion between a dynamic table and an external connector. 
The update mode specifies which kind of messages should be exchanged with the external system:

对于流查询，需要声明如何在动态表和外部连接器之间执行转换。的更新模式指定哪些类型的消息应与外部系统进行交换：

Append Mode: In append mode, a dynamic table and an external connector only exchange INSERT messages.

追加模式：在追加模式下，动态表和外部连接器仅通过INSERT交互消息。

Retract Mode: In retract mode, a dynamic table and an external connector exchange ADD and RETRACT messages. 
An INSERT change is encoded as an ADD message, a DELETE change as a RETRACT message, and an UPDATE change as a RETRACT message for the updated (previous) row and an ADD message for the updating (new) row. 
In this mode, a key must not be defined as opposed to upsert mode. However, every update consists of two messages which is less efficient.

缩回模式：在缩回模式下，动态表和外部连接器交换ADD和RETRACT消息。INSERT更改被编码为ADD消息，DELETE更改为RETRACT消息，UPDATE更改为更新（先前）行的RETRACT消息和ADD消息（更新）行的ADD消息。
在此模式下，与upsert模式相反，不得定义密钥。但是，每个更新都包含两个消息，导致效率较低。

Upsert Mode: In upsert mode, a dynamic table and an external connector exchange UPSERT and DELETE messages. This mode requires a (possibly composite) unique key by which updates can be propagated. 
The external connector needs to be aware of the unique key attribute in order to apply messages correctly. INSERT and UPDATE changes are encoded as UPSERT messages. DELETE changes as DELETE messages. 
The main difference to a retract stream is that UPDATE changes are encoded with a single message and are therefore more efficient.

Upsert模式：在Upsert模式下，动态表和外部连接器交换UPSERT和DELETE消息。此模式需要一个（可能是复合的）唯一密钥，通过该密钥可以传播更新。外部连接器需要唯一键属性，才能正确应用消息。
INSERT和UPDATE更改被编码为UPSERT消息。DELETE更改为DELETE消息。与撤回流的主要区别在于UPDATE更改是用单个消息编码的，因此效率更高。

Attention ： The documentation of each connector states which update modes are supported.

注意：每个连接器的文档都说明了支持哪些更新模式。

```text
CREATE TABLE MyTable (
 ...
) WITH (
 'update-mode' = 'append'  -- otherwise: 'retract' or 'upsert'
)
```

```text
.connect(...)
  .inAppendMode()    // otherwise: inUpsertMode() or inRetractMode()
```

```text
tables:
  - name: ...
    update-mode: append    # otherwise: "retract" or "upsert"
```

See also the general streaming concepts documentation for more information. 

另请参阅常规流概念文档。 

### Table Connectors 

Flink provides a set of connectors for connecting to external systems.

Flink提供了一组用于连接到外部系统的连接器。

Please note that not all connectors are available in both batch and streaming yet. Furthermore, not every streaming connector supports every streaming mode. 
Therefore, each connector is tagged accordingly. A format tag indicates that the connector requires a certain type of format.

请注意，并非所有连接器都可以批量和流式使用。此外，并非每个流连接器都支持每种流模式。因此，每个连接器都有相应的标记。格式标签表示连接器需要某种类型的格式。

### Kafka Connector 

```text
Source: Streaming Append Mode 
Sink: Streaming Append Mode 
Format: CSV, JSON, Avro
```

The Kafka connector allows for reading and writing from and to an Apache Kafka topic. It can be defined as follows:

```text
CREATE TABLE MyUserTable (
  ...
) WITH (
  'connector.type' = 'kafka',       

  'connector.version' = '0.11',     -- required: valid connector versions are
                                    -- "0.8", "0.9", "0.10", "0.11", and "universal"

  'connector.topic' = 'topic_name', -- required: topic name from which the table is read

  'connector.properties.zookeeper.connect' = 'localhost:2181', -- required: specify the ZooKeeper connection string
  'connector.properties.bootstrap.servers' = 'localhost:9092', -- required: specify the Kafka server connection string
  'connector.properties.group.id' = 'testGroup', --optional: required in Kafka consumer, specify consumer group
  'connector.startup-mode' = 'earliest-offset',    -- optional: valid modes are "earliest-offset", 
                                                   -- "latest-offset", "group-offsets", 
                                                   -- or "specific-offsets"

  -- optional: used in case of startup mode with specific offsets
  'connector.specific-offsets' = 'partition:0,offset:42;partition:1,offset:300',

  'connector.sink-partitioner' = '...',  -- optional: output partitioning from Flink's partitions 
                                         -- into Kafka's partitions valid are "fixed" 
                                         -- (each Flink partition ends up in at most one Kafka partition),
                                         -- "round-robin" (a Flink partition is distributed to 
                                         -- Kafka partitions round-robin)
                                         -- "custom" (use a custom FlinkKafkaPartitioner subclass)
  -- optional: used in case of sink partitioner custom
  'connector.sink-partitioner-class' = 'org.mycompany.MyPartitioner',
  
  'format.type' = '...',                 -- required: Kafka connector requires to specify a format,
  ...                                    -- the supported formats are 'csv', 'json' and 'avro'.
                                         -- Please refer to Table Formats section for more details.
)
```

```text
.connect(
  new Kafka()
    .version("0.11")    // required: valid connector versions are
                        //   "0.8", "0.9", "0.10", "0.11", and "universal"
    .topic("...")       // required: topic name from which the table is read

    // optional: connector specific properties
    .property("zookeeper.connect", "localhost:2181")
    .property("bootstrap.servers", "localhost:9092")
    .property("group.id", "testGroup")

    // optional: select a startup mode for Kafka offsets
    .startFromEarliest()
    .startFromLatest()
    .startFromSpecificOffsets(...)

    // optional: output partitioning from Flink's partitions into Kafka's partitions
    .sinkPartitionerFixed()         // each Flink partition ends up in at-most one Kafka partition (default)
    .sinkPartitionerRoundRobin()    // a Flink partition is distributed to Kafka partitions round-robin
    .sinkPartitionerCustom(MyCustom.class)    // use a custom FlinkKafkaPartitioner subclass
)
.withFormat(                                  // required: Kafka connector requires to specify a format,
  ...                                         // the supported formats are Csv, Json and Avro.
)                                             // Please refer to Table Formats section for more details.
```

Specify the start reading position: By default, the Kafka source will start reading data from the committed group offsets in Zookeeper or Kafka brokers. 
You can specify other start positions, which correspond to the configurations in section Kafka Consumers Start Position Configuration.

指定开始读取位置：默认情况下，Kafka源将开始从Zookeeper或Kafka代理中的已提交组偏移量读取数据。您可以指定其他起始位置，这些位置与“ Kafka消费者起始位置配置”部分中的配置相对应。

Flink-Kafka Sink Partitioning: By default, a Kafka sink writes to at most as many partitions as its own parallelism (each parallel instance of the sink writes to exactly one partition). 
In order to distribute the writes to more partitions or control the routing of rows into partitions, a custom sink partitioner can be provided. The round-robin partitioner is useful to avoid an unbalanced partitioning. 
However, it will cause a lot of network connections between all the Flink instances and all the Kafka brokers.

Flink-Kafka接收器分区：默认情况下，Kafka接收器最多可写入与其自身并行度相同的分区（每个并行的接收器实例均写入一个分区）。
为了将写操作分配到更多分区或控制行到分区的路由，可以提供自定义接收器分区程序。循环分区器对于避免不平衡分区很有用。但是，这将导致所有Flink实例与所有Kafka代理之间的大量网络连接。

Consistency guarantees: By default, a Kafka sink ingests data with at-least-once guarantees into a Kafka topic if the query is executed with checkpointing enabled.

一致性保证：默认情况下，如果在启用了检查点的情况下执行查询，则Kafka接收器会将具有至少一次保证的数据提取到Kafka主题中。

Kafka 0.10+ Timestamps: Since Kafka 0.10, Kafka messages have a timestamp as metadata that specifies when the record was written into the Kafka topic. 
These timestamps can be used for a rowtime attribute by selecting timestamps: from-source in YAML and timestampsFromSource() in Java/Scala respectively.

Kafka 0.10+时间戳：自Kafka 0.10起，Kafka消息具有时间戳作为元数据，该时间戳指定何时将记录写入Kafka主题。
通过分别在YAML和Java / Scala中进行选择，可以将这些时间戳用于rowtime属性。timestamps: from-sourcetimestampsFromSource()

Kafka 0.11+ Versioning: Since Flink 1.7, the Kafka connector definition should be independent of a hard-coded Kafka version. 
Use the connector version universal as a wildcard for Flink’s Kafka connector that is compatible with all Kafka versions starting from 0.11.

Kafka 0.11+版本：自Flink 1.7起，Kafka连接器定义应独立于硬编码的Kafka版本。使用连接器版本universal作为Flink Kafka连接器的通配符，该连接器与所有从0.11开始的Kafka版本兼容。

Make sure to add the version-specific Kafka dependency. In addition, a corresponding format needs to be specified for reading and writing rows from and to Kafka.

确保添加特定于版本的Kafka依赖项。另外，需要指定一种相应的格式来读写Kafka中的行。

### Table Formats 

Flink provides a set of table formats that can be used with table connectors.

Flink提供了一组表格式，可与表连接器一起使用。

A format tag indicates the format type for matching with a connector.

格式标签表示与连接器匹配的格式类型。 

#### JSON Format

```text
Format: Serialization Schema 
Format: Deserialization Schema
```

The JSON format allows to read and write JSON data that corresponds to a given format schema. The format schema can be defined either as a Flink type, as a JSON schema, or derived from the desired table schema. 
A Flink type enables a more SQL-like definition and mapping to the corresponding SQL data types. The JSON schema allows for more complex and nested structures.

JSON格式允许读取和写入与给定格式架构相对应的JSON数据。格式模式可以定义为Flink类型，JSON模式或从所需的表模式派生。Flink类型启用了更类似于SQL的定义并映射到相应的SQL数据类型。JSON模式允许更复杂和嵌套的结构。

If the format schema is equal to the table schema, the schema can also be automatically derived. This allows for defining schema information only once. The names, types, and fields’ order of the format are determined by the table’s schema. Time attributes are ignored if their origin is not a field. A from definition in the table schema is interpreted as a field renaming in the format.

如果格式模式等于表模式，则也可以自动派生该模式。这只允许定义一次架构信息。格式的名称，类型和字段的顺序由表的架构确定。如果时间属性的来源不是字段，则将忽略它们。一个from表中的模式定义解释为格式字段命名。

The JSON format can be used as follows:

JSON格式可以如下使用：

```text
CREATE TABLE MyUserTable (
  ...
) WITH (
  'format.type' = 'json',                   -- required: specify the format type
  'format.fail-on-missing-field' = 'true'   -- optional: flag whether to fail if a field is missing or not, false by default

  'format.fields.0.name' = 'lon',           -- optional: define the schema explicitly using type information.
  'format.fields.0.data-type' = 'FLOAT',    -- This overrides default behavior that uses table's schema as format schema.
  'format.fields.1.name' = 'rideTime',
  'format.fields.1.data-type' = 'TIMESTAMP(3)',

  'format.json-schema' =                    -- or by using a JSON schema which parses to DECIMAL and TIMESTAMP.
    '{                                      -- This also overrides the default behavior.
      "type": "object",
      "properties": {
        "lon": {
          "type": "number"
        },
        "rideTime": {
          "type": "string",
          "format": "date-time"
        }
      }
    }'
)
```

```text
.withFormat(
  new Json()
    .failOnMissingField(true)   // optional: flag whether to fail if a field is missing or not, false by default

    // optional: define the schema explicitly using type information. This overrides default
    // behavior that uses table's schema as format schema.
    .schema(Type.ROW(...))

    // or by using a JSON schema which parses to DECIMAL and TIMESTAMP. This also overrides default behavior.
    .jsonSchema(
      "{" +
      "  type: 'object'," +
      "  properties: {" +
      "    lon: {" +
      "      type: 'number'" +
      "    }," +
      "    rideTime: {" +
      "      type: 'string'," +
      "      format: 'date-time'" +
      "    }" +
      "  }" +
      "}"
    )
)
```

```text
format:
  type: json
  fail-on-missing-field: true   # optional: flag whether to fail if a field is missing or not, false by default

  # optional: define the schema explicitly using type information. This overrides default
  # behavior that uses table's schema as format schema.
  schema: "ROW(lon FLOAT, rideTime TIMESTAMP)"

  # or by using a JSON schema which parses to DECIMAL and TIMESTAMP. This also overrides the default behavior.
  json-schema: >
    {
      type: 'object',
      properties: {
        lon: {
          type: 'number'
        },
        rideTime: {
          type: 'string',
          format: 'date-time'
        }
      }
    }
```

The following table shows the mapping of JSON schema types to Flink SQL types: 

下表显示了JSON模式类型到Flink SQL类型的映射：

```text

JSON schema	Flink SQL
object	ROW
boolean	BOOLEAN
array	ARRAY[_]
number	DECIMAL
integer	DECIMAL
string	STRING
string with format: date-time	TIMESTAMP
string with format: date	DATE
string with format: time	TIME
string with encoding: base64	ARRAY[TINYINT]
null	NULL (unsupported yet)
```

Currently, Flink supports only a subset of the JSON schema specification draft-07. Union types (as well as allOf, anyOf, not) are not supported yet. oneOf and arrays of types are only supported for specifying nullability.

Simple references that link to a common definition in the document are supported as shown in the more complex example below:

当前，Flink仅支持JSON模式规范 的子集draft-07。Union类型（以及allOf，anyOf，not）尚未支持。oneOf和数组类型仅用于指定可为空性。

支持链接到文档中通用定义的简单引用，如以下更复杂的示例所示：

```text
{
  "definitions": {
    "address": {
      "type": "object",
      "properties": {
        "street_address": {
          "type": "string"
        },
        "city": {
          "type": "string"
        },
        "state": {
          "type": "string"
        }
      },
      "required": [
        "street_address",
        "city",
        "state"
      ]
    }
  },
  "type": "object",
  "properties": {
    "billing_address": {
      "$ref": "#/definitions/address"
    },
    "shipping_address": {
      "$ref": "#/definitions/address"
    },
    "optional_address": {
      "oneOf": [
        {
          "type": "null"
        },
        {
          "$ref": "#/definitions/address"
        }
      ]
    }
  }
}
```

Missing Field Handling: By default, a missing JSON field is set to null. You can enable strict JSON parsing that will cancel the source (and query) if a field is missing.

Make sure to add the JSON format as a dependency.

缺少字段处理：默认情况下，缺少JSON字段设置为null。您可以启用严格的JSON解析，如果缺少字段，该解析将取消源（和查询）。

确保将JSON格式添加为依赖项。


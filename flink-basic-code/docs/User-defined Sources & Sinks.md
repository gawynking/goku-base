
# User-defined Sources & Sinks 


A TableSource provides access to data which is stored in external systems (database, key-value store, message queue) or files. 
After a TableSource is registered in a TableEnvironment it can be accessed by Table API or SQL queries.

TableSource提供对存储在外部系统（数据库，键值存储，消息队列）或文件中的数据的访问。在TableEnvironment中注册TableSource后，可以通过Table API或SQL查询对其进行访问。

A TableSink emits a Table to an external storage system, such as a database, key-value store, message queue, or file system (in different encodings, e.g., CSV, Parquet, or ORC).

TableSink 将表发送到外部存储系统，例如数据库，键值存储，消息队列或文件系统（采用不同的编码，例如CSV，Parquet或ORC）。

A TableFactory allows for separating the declaration of a connection to an external system from the actual implementation. 
A table factory creates configured instances of table sources and sinks from normalized, string-based properties. 
The properties can be generated programmatically using a Descriptor or via YAML configuration files for the SQL Client.

TableFactory允许将与外部系统的连接的声明与实际实现分开。表工厂从标准化的基于字符串的属性创建表源和表接收器配置实例。可以通过SQL Client的YAML配置文件以编程方式生成属性。

Have a look at the common concepts and API page for details how to register a TableSource and how to emit a Table through a TableSink. See the built-in sources, sinks, and formats page for examples how to use factories.

看一下通用概念和API页面，详细了解如何注册TableSource以及如何通过TableSink发出表。有关如何使用工厂的示例，请参见内置的源，接收器和格式页面。

## Define a TableSource 

A TableSource is a generic interface that gives Table API and SQL queries access to data stored in an external system. It provides the schema of the table and the records that are mapped to rows with the table’s schema. 
Depending on whether the TableSource is used in a streaming or batch query, the records are produced as a DataSet or DataStream.

TableSource是一个通用接口，使Table API和SQL查询可以访问存储在外部系统中的数据。它提供了表的schema以及与该表的schema映射到行的记录。根据TableSource是在流查询还是批处理查询中使用，记录将生成为DataSet或DataStream。

If a TableSource is used in a streaming query it must implement the StreamTableSource interface, if it is used in a batch query it must implement the BatchTableSource interface. 
A TableSource can also implement both interfaces and be used in streaming and batch queries.

如果TableSource在流查询中使用，则必须实现该StreamTableSource接口，如果在批处理查询中使用，则必须实现该BatchTableSource接口。TableSource还可以同时实现两个接口，并且可以在流查询和批处理查询中使用。

StreamTableSource and BatchTableSource extend the base interface TableSource that defines the following methods:

StreamTableSource和BatchTableSource扩展基础接口TableSource定义以下方法：

```text
TableSource<T> {

  public TableSchema getTableSchema();

  public TypeInformation<T> getReturnType();

  public String explainSource();
}
```

--getTableSchema(): Returns the schema of the produced table, i.e., the names and types of the fields of the table. The field types are defined using Flink’s DataType (see Table API types and SQL types). 
Note that the returned TableSchema shouldn’t contain computed columns to reflect the schema of the physical TableSource.
- getTableSchema()：返回生成的表的schema，即表的字段的名称和类型。字段类型是使用Flink DataType定义的（请参见Table API类型和SQL类型）。请注意，返回的TableSchema 不应包含计算所得的列去映射TableSource物理schema。
- getReturnType(): Returns the physical type of the DataStream (StreamTableSource) or DataSet (BatchTableSource) and the records that are produced by the TableSource.
- getReturnType()：返回DataStream（StreamTableSource）或DataSet（BatchTableSource）的物理类型以及产生的记录TableSource。
- explainSource(): Returns a String that describes the TableSource. This method is optional and used for display purposes only.
- explainSource()：返回TableSource的描述字符串。此方法是可选的，仅用于显示目的。


The TableSource interface separates the logical table schema from the physical type of the returned DataStream or DataSet. 
As a consequence, all fields of the table schema (getTableSchema()) must be mapped to a field with corresponding type of the physical return type (getReturnType()). 
By default, this mapping is done based on field names. 
For example, a TableSource that defines a table schema with two fields [name: String, size: Integer] requires a TypeInformation with at least two fields called name and size of type String and Integer, respectively. 
This could be a PojoTypeInfo or a RowTypeInfo that have two fields named name and size with matching types.

TableSource接口将逻辑表schema与返回的DataStream或DataSet的物理类型分开。
因此，表shcmea（getTableSchema()）的所有字段都必须映射到具有物理返回类型（getReturnType()）的相应类型的字段。默认情况下，此映射是基于字段名称完成的。
例如，TableSource定义一个具有两个字段的表模式的[name: String, size: Integer]，因此需要一个至少包含两个名为name和size的字段，并且数据类型String和Integer。
可以通过PojoTypeInfo或RowTypeInfo方式，定义具有两个名为 name 和 size的匹配类型的字段。

However, some types, such as Tuple or CaseClass types, do support custom field names. 
If a TableSource returns a DataStream or DataSet of a type with fixed field names, it can implement the DefinedFieldMapping interface to map field names from the table schema to field names of the physical return type.

但是，某些类型（例如Tuple或CaseClass类型）确实支持自定义字段名称。
如果TableSource返回具有固定字段名称和类型的DataStream或DataSet，则它可以实现DefinedFieldMapping接口以将表schema中的字段名称映射到物理返回类型的字段名称。

### Defining a BatchTableSource

The BatchTableSource interface extends the TableSource interface and defines one additional method:

BatchTableSource接口扩展了TableSource接口，并定义一个额外的方法：

```text
BatchTableSource<T> implements TableSource<T> {
  public DataSet<T> getDataSet(ExecutionEnvironment execEnv);
}
```

getDataSet(execEnv): Returns a DataSet with the data of the table. The type of the DataSet must be identical to the return type defined by the TableSource.getReturnType() method. 
The DataSet can by created using a regular data source of the DataSet API. Commonly, a BatchTableSource is implemented by wrapping a InputFormat or batch connector.

getDataSet(execEnv)：返回带有table格式数据的DataSet。DataSet的类型必须与TableSource.getReturnType()方法定义的返回类型相同。
在DataSet通过使用DataSet API的常规数据源创建。通常，BatchTableSource是通过包装InputFormat或批处理连接器实现的。

### Defining a StreamTableSource 

The StreamTableSource interface extends the TableSource interface and defines one additional method: 

StreamTableSource接口集成了TableSource接口，并定义一个额外的方法： 

```text
StreamTableSource<T> implements TableSource<T> {
  public DataStream<T> getDataStream(StreamExecutionEnvironment execEnv);
}
```

getDataStream(execEnv): Returns a DataStream with the data of the table. The type of the DataStream must be identical to the return type defined by the TableSource.getReturnType() method. 
The DataStream can by created using a regular data source of the DataStream API. Commonly, a StreamTableSource is implemented by wrapping a SourceFunction or a stream connector.

getDataStream(execEnv)：返回table类型的DataStream。DataStream的类型必须与TableSource.getReturnType()方法定义的返回类型相同。在DataStream通过使用DataStream API的常规数据源创建。
通常，StreamTableSource是通过包装SourceFunction或流连接器来实​​现的。


### Defining a TableSource with Time Attributes 

Time-based operations of streaming Table API and SQL queries, such as windowed aggregations or joins, require explicitly specified time attributes.

基于时间的操作的流表API和SQL查询，（例如windowed aggregations or joins）需要显式指定的时间属性。

A TableSource defines a time attribute as a field of type Types.SQL_TIMESTAMP in its table schema. 
In contrast to all regular fields in the schema, a time attribute must not be matched to a physical field in the return type of the table source. 
Instead, a TableSource defines a time attribute by implementing a certain interface.

TableSource通过 Types.SQL_TIMESTAMP 在table schema中定义一个字段类型作为时间属性。与schema中的所有常规字段相反，时间属性不得与表源的返回类型中的物理字段匹配。而是TableSource通过实现某个接口来定义时间属性。

#### Defining a Processing Time Attribute 

Processing time attributes are commonly used in streaming queries. A processing time attribute returns the current wall-clock time of the operator that accesses it. 
A TableSource defines a processing time attribute by implementing the DefinedProctimeAttribute interface. The interface looks as follows:

处理时间属性通常用于流查询中。处理时间属性返回访问该属性的算子的当前挂钟时间。TableSource通过实现DefinedProctimeAttribute接口来定义处理时间属性。该界面如下所示：

```text
DefinedProctimeAttribute {
  public String getProctimeAttribute();
}
```

getProctimeAttribute(): Returns the name of the processing time attribute. The specified attribute must be defined of type Types.SQL_TIMESTAMP in the table schema and can be used in time-based operations. 
A DefinedProctimeAttribute table source can define no processing time attribute by returning null.

getProctimeAttribute()：返回处理时间属性的名称。指定的属性必须以Types.SQL_TIMESTAMP在表schema中定义的类型，并且可以在基于时间的操作中使用。DefinedProctimeAttribute表源定义没有处理时间被返回的属性为null。

Attention ： Both StreamTableSource and BatchTableSource can implement DefinedProctimeAttribute and define a processing time attribute. 
In case of a BatchTableSource the processing time field is initialized with the current timestamp during the table scan.

注意 ： StreamTableSource和BatchTableSource可以实现DefinedProctimeAttribute并定义的处理时间属性。在BatchTableSource表扫描期间，使用当前时间戳初始化处理时间字段。


#### Defining a Rowtime Attribute 

Rowtime attributes are attributes of type TIMESTAMP and handled in a unified way in stream and batch queries.

行时间属性是TIMESTAMP类型的属性，在流查询和批处理查询中以统一的方式处理。

A table schema field of type SQL_TIMESTAMP can be declared as rowtime attribute by specifying

table schema中的SQL_TIMESTAMP类型的字段可以被指定为一个rowtime属性

the name of the field：
a TimestampExtractor that computes the actual value for the attribute (usually from one or more other fields), and
a WatermarkStrategy that specifies how watermarks are generated for the the rowtime attribute.
A TableSource defines a rowtime attribute by implementing the DefinedRowtimeAttributes interface. The interface looks as follows:

字段名称：
一个TimestampExtractor计算属性的实际值（通常从一个或多个其他字段），并一个WatermarkStrategy用于指定如何为rowtime属性生成watermark。
A TableSource通过实现DefinedRowtimeAttributes接口来定义行时间属性。该接口如下所示：

```text
DefinedRowtimeAttribute {
  public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors();
}
```

getRowtimeAttributeDescriptors(): Returns a list of RowtimeAttributeDescriptor. A RowtimeAttributeDescriptor describes a rowtime attribute with the following properties:

getRowtimeAttributeDescriptors()：返回一个RowtimeAttributeDescriptor的列表。A RowtimeAttributeDescriptor描述了具有以下属性的行时间属性：

attributeName: The name of the rowtime attribute in the table schema. The field must be defined with type Types.SQL_TIMESTAMP.

attributeName：table schema中的rowtime属性的名称。该字段必须使用Types.SQL_TIMESTAMP类型定义。 

timestampExtractor: The timestamp extractor extracts the timestamp from a record with the return type. For example, it can convert a Long field into a timestamp or parse a String-encoded timestamp. 
Flink comes with a set of built-in TimestampExtractor implementation for common use cases. It is also possible to provide a custom implementation.

timestampExtractor：时间戳提取器从具有返回类型的记录中提取时间戳。例如，它可以将Long字段转换为时间戳，或者解析String编码的时间戳。
Flink带有一组TimestampExtractor针对常见用例的内置实现。也可以提供自定义实现。

watermarkStrategy: The watermark strategy defines how watermarks are generated for the rowtime attribute. Flink comes with a set of built-in WatermarkStrategy implementations for common use cases. 
It is also possible to provide a custom implementation.

watermarkStrategy：水印策略定义了如何为rowtime属性生成水印。Flink带有一组WatermarkStrategy用于常见用例的内置实现。也可以提供自定义实现。


Attention ： Although the getRowtimeAttributeDescriptors() method returns a list of descriptors, only a single rowtime attribute is support at the moment. 
We plan to remove this restriction in the future and support tables with more than one rowtime attribute.

注意：尽管该getRowtimeAttributeDescriptors()方法返回一个描述符列表，但目前仅支持单个rowtime属性。我们计划将来删除此限制，并支持具有多个rowtime属性的表。

Attention ： Both, StreamTableSource and BatchTableSource, can implement DefinedRowtimeAttributes and define a rowtime attribute. In either case, the rowtime field is extracted using the TimestampExtractor. 
Hence, a TableSource that implements StreamTableSource and BatchTableSource and defines a rowtime attribute provides exactly the same data to streaming and batch queries.

注意：两者，StreamTableSource和BatchTableSource，可以实现DefinedRowtimeAttributes并定义rowtime属性。无论哪种情况，都使用TimestampExtractor来提取rowtime字段。
因此，TableSource实现StreamTableSource和BatchTableSource并且定义rowtime属性的，可以为流查询和批处理查询提供完全相同的数据。


Provided Timestamp Extractors -- 提供的时间戳提取器 
Flink provides TimestampExtractor implementations for common use cases. -- Flink提供TimestampExtractor了常见用例的实现。 

The following TimestampExtractor implementations are currently available: -- TimestampExtractor当前提供以下实现： 

ExistingField(fieldName): Extracts the value of a rowtime attribute from an existing LONG, SQL_TIMESTAMP, or timestamp formatted STRING field. One example of such a string would be ‘2018-05-28 12:34:56.000’.

ExistingField(fieldName)：从LONG，SQL_TIMESTAMP或时间戳记格式化STRING类型的字段提取时间戳属性值。这样的字符串的一个示例是“ 2018-05-28 12：34：56.000”。

StreamRecordTimestamp(): Extracts the value of a rowtime attribute from the timestamp of the DataStream StreamRecord. Note, this TimestampExtractor is not available for batch table sources.
A custom TimestampExtractor can be defined by implementing the corresponding interface.

StreamRecordTimestamp()：提取rowtime属性从DataStream StreamRecord类型时间戳。注意，这TimestampExtractor不适用于批处理表源。TimestampExtractor可以通过实现相应的接口来自定义。


Provided Watermark Strategies -- 提供的水印策略

Flink provides WatermarkStrategy implementations for common use cases. -- Flink提供WatermarkStrategy的常见用例的实现。

The following WatermarkStrategy implementations are currently available: -- WatermarkStrategy当前提供以下实现：

AscendingTimestamps: A watermark strategy for ascending timestamps. Records with timestamps that are out-of-order will be considered late.

AscendingTimestamps：提升时间戳的水印策略。时间戳不正确的记录将被视为较晚。

BoundedOutOfOrderTimestamps(delay): A watermark strategy for timestamps that are at most out-of-order by the specified delay.

BoundedOutOfOrderTimestamps(delay)：用于时间戳的水印策略，该时间戳最多按指定的延迟乱序。 

PreserveWatermarks(): A strategy which indicates the watermarks should be preserved from the underlying DataStream.

PreserveWatermarks()：从底层保留DataStream指示水印的策略。 

A custom WatermarkStrategy can be defined by implementing the corresponding interface.

WatermarkStrategy可以通过实现相应的接口来自定义。


### Defining a TableSource with Projection Push-Down 

A TableSource supports projection push-down by implementing the ProjectableTableSource interface. The interface defines a single method: 

TableSource通过实现ProjectableTableSource接口来支持投影下推。该接口定义了一个方法： 

```text
ProjectableTableSource<T> {
  public TableSource<T> projectFields(int[] fields);
}
```

projectFields(fields): Returns a copy of the TableSource with adjusted physical return type. The fields parameter provides the indexes of the fields that must be provided by the TableSource. 
The indexes relate to the TypeInformation of the physical return type, not to the logical table schema. The copied TableSource must adjust its return type and the returned DataStream or DataSet. 
The TableSchema of the copied TableSource must not be changed, i.e, it must be the same as the original TableSource. 
If the TableSource implements the DefinedFieldMapping interface, the field mapping must be adjusted to the new return type.

projectFields(fields)：返回一个TableSource调整过的物理类型副本。fields参数有tablesource字段索引提供。索引与物理返回类型TypeInformation有关，而不与逻辑表模式有关。
TableSource的副本必须调整其返回类型，然后返回DataStream或DataSet。TableSource副本的TableSchema不能改变，即它必须跟原来一样TableSource。
如果TableSource实现DefinedFieldMapping接口，则必须将字段映射调整为新的返回类型。

Attention ： In order for Flink to distinguish a projection push-down table source from its original form, explainSource method must be override to include information regarding the projected fields.

注意：为了使Flink可以将投影下推表源与其原始形式区分开，explainSource必须重写方法以包括有关投影字段的信息。

The ProjectableTableSource adds support to project flat fields. If the TableSource defines a table with nested schema, it can implement the NestedFieldsProjectableTableSource to extend the projection to nested fields. 
The NestedFieldsProjectableTableSource is defined as follows:

该ProjectableTableSource支持投影字段的展平。如果TableSource定义具有嵌套schema的表，则可以实现NestedFieldsProjectableTableSource以将展开投影嵌套字段。NestedFieldsProjectableTableSource定义如下：

```text
NestedFieldsProjectableTableSource<T> {
  public TableSource<T> projectNestedFields(int[] fields, String[][] nestedFields);
}
```

projectNestedField(fields, nestedFields): Returns a copy of the TableSource with adjusted physical return type. Fields of the physical return type may be removed or reordered but their type must not be changed. 
The contract of this method is essentially the same as for the ProjectableTableSource.projectFields() method. 
In addition, the nestedFields parameter contains for each field index in the fields list, a list of paths to all nested fields that are accessed by the query. 
All other nested fields do not need to be read, parsed, and set in the records that are produced by the TableSource.

projectNestedField(fields, nestedFields)：返回TableSource包含调整过的物理返回类型一个副本的。物理返回类型的字段可以删除或重新排序，但不得更改其类型。
该方法本质上与ProjectableTableSource.projectFields()方法相同。此外，nestedFields包含fields列表中的每个字段索引，该列表包含查询访问的所有嵌套字段的路径的列表。
产生TableSource记录的所有其他嵌套字段都不需要记录中读取，解析和设置。

Attention : the types of the projected fields must not be changed but unused fields may be set to null or to a default value.

注意 : 不得更改投影字段的类型，但未使用的字段可以设置为null或默认值。


### Defining a TableSource with Filter Push-Down 

The FilterableTableSource interface adds support for filter push-down to a TableSource. A TableSource extending this interface is able to filter records such that the returned DataStream or DataSet returns fewer records.

FilterableTableSource接口增加了对过滤器下推到的支持相对于TableSource而言。一个TableSource扩展这个接口能够过滤记录，从而使DataStream或者DataSet返回较少的记录。


The interface looks as follows:

该界面如下所示：

```text
FilterableTableSource<T> {
  public TableSource<T> applyPredicate(List<Expression> predicates);
  public boolean isFilterPushedDown();
}
```

applyPredicate(predicates): Returns a copy of the TableSource with added predicates. The predicates parameter is a mutable list of conjunctive predicates that are “offered” to the TableSource. 
The TableSource accepts to evaluate a predicate by removing it from the list. Predicates that are left in the list will be evaluated by a subsequent filter operator.

applyPredicate(predicates)：返回一个添加了谓词的TableSource副本。predicates参数 是一个可变列表 包含连接谓词 如  "offered" 的TableSource。TableSource受理用户从列表中删除它来评估一个谓语。列表中剩余的谓词将由后续的过滤器运算符评估。

isFilterPushedDown(): Returns true if the applyPredicate() method was called before. Hence, isFilterPushedDown() must return true for all TableSource instances returned from a applyPredicate() call.

isFilterPushedDown()：如果以前调用过applyPredicate()方法，则返回true 。因此，isFilterPushedDown()对于从applyPredicate()调用返回的所有TableSource实例，必须返回true 。


Attention ： In order for Flink to distinguish a filter push-down table source from its original form, explainSource method must be override to include information regarding the push-down filters.

注意：为了使Flink能够将过滤器下推表源与其原始形式区分开，explainSource必须重写方法以包括有关下推式过滤器的信息。


### Defining a TableSource for Lookups  -- 可用于维表join，目前支持尚不完善 

Attention ： This is an experimental feature. The interface may be changed in future versions. It’s only supported in Blink planner.

注意 ： 这是一项实验功能。这个接口将来的版本中可能会更改。仅在Blink planner中受支持。

The LookupableTableSource interface adds support for the table to be accessed via key column(s) in a lookup fashion. This is very useful when used to join with a dimension table to enrich some information. 
If you want to use the TableSource in lookup mode, you should use the source in temporal table join syntax.

LookupableTableSource接口 增加了 对要通过查找方式通过键列访问表的支持。当用于与维表join以获得丰富信息时，这非常有用。如果要TableSource在lookup模式下使用，则应在时态表联接语法中使用源。

The interface looks as follows:

该接口如下所示：

```text
LookupableTableSource<T> implements TableSource<T> {
  public TableFunction<T> getLookupFunction(String[] lookupkeys);
  public AsyncTableFunction<T> getAsyncLookupFunction(String[] lookupkeys);
  public boolean isAsyncEnabled();
}
```

getLookupFunction(lookupkeys): Returns a TableFunction which used to lookup the matched row(s) via lookup keys. 
The lookupkeys are the field names of LookupableTableSource in the join equal conditions. 

The eval method parameters of the returned TableFunction’s should be in the order which lookupkeys defined. 
It is recommended to define the parameters in varargs (e.g. eval(Object... lookupkeys) to match all the cases). 
The return type of the TableFunction must be identical to the return type defined by the TableSource.getReturnType() method.

getLookupFunction(lookupkeys)：返回用于通过查找键查找匹配行的TableFunction。lookupkeys是LookupableTableSource在等值join中的一个字段名称。
eval方法参数应按TableFunction方法的lookupkeys定义的顺序。建议在varargs中定义参数（例如eval(Object... lookupkeys)，匹配所有情况）。
TableFunction的返回类型必须与TableSource.getReturnType()方法定义的返回类型相同。

getAsyncLookupFunction(lookupkeys): Optional. Similar to getLookupFunction, but the AsyncLookupFunction lookups the matched row(s) asynchronously. 
The underlying of AsyncLookupFunction will be called via Async I/O.
The first argument of the eval method of the returned AsyncTableFunction should be defined as java.util.concurrent.CompletableFuture to collect results asynchronously (e.g. eval(CompletableFuture<Collection<String>> result, 
Object... lookupkeys)). The implementation of this method can throw an exception if the TableSource doesn’t support asynchronously lookup.

getAsyncLookupFunction(lookupkeys)： 可选的。与相似getLookupFunction，但是AsyncLookupFunction查找查询匹配的行是异步的。
优先于AsyncLookupFunction将通过Async I/O调用。
AsyncTableFunction返回的eval方法的第一个参数应定义做为java.util.concurrent.CompletableFuture去异步收集结果（例如eval(CompletableFuture<Collection<String>> result, Object... lookupkeys)）。
如果TableSource不支持异步查找，则此方法的实现可能引发异常。

isAsyncEnabled(): Returns true if async lookup is enabled. It requires getAsyncLookupFunction(lookupkeys) is implemented if isAsyncEnabled returns true.

isAsyncEnabled()：如果启用了异步查找，则返回true。如果getAsyncLookupFunction(lookupkeys)的被实现了，则isAsyncEnabled返回true。

## Define a TableSink 

A TableSink specifies how to emit a Table to an external system or location. The interface is generic such that it can support different storage locations and formats. 
There are different table sinks for batch tables and streaming tables.

TableSink指定如何向外部系统或location发送一张table。该接口是通用的，因此它可以支持不同的存储位置和格式。批处理表和流式表有不同的表接收器。

The general interface looks as follows:

常规接口如下所示：

```text
TableSink<T> {
  public TypeInformation<T> getOutputType();
  public String[] getFieldNames();
  public TypeInformation[] getFieldTypes();
  public TableSink<T> configure(String[] fieldNames, TypeInformation[] fieldTypes);
}
```

The TableSink#configure method is called to pass the schema of the Table (field names and types) to emit to the TableSink. 
The method must return a new instance of the TableSink which is configured to emit the provided Table schema. 
Note that the provided TableSchema shouldn’t contain computed columns to reflect the schema of the physical TableSink.

TableSink#configure方法被调用可将Table的schema（字段名称和类型）传递给TableSink。该方法必须返回TableSink的新实例，该实例被配置为提供发出的Table模式。请注意，提供的内容TableSchema不应包含计算所得的列以反映TableSink的物理架构。


### BatchTableSink 

Defines an external TableSink to emit a batch table.

The interface looks as follows:

定义一个外部TableSink来发出批处理表。

该接口如下所示：

```text
BatchTableSink<T> implements TableSink<T> {
  public void emitDataSet(DataSet<T> dataSet);
}
```

### AppendStreamTableSink 

Defines an external TableSink to emit a streaming table with only insert changes.

定义TableSink用于发出仅包含插入类型外部流表对象。

The interface looks as follows:

该接口如下所示：

```text
AppendStreamTableSink<T> implements TableSink<T> {
  public void emitDataStream(DataStream<T> dataStream);
}
```

If the table is also modified by update or delete changes, a TableException will be thrown. 

如果还通过更新或删除更改来修改表，TableException则将引发。 


### RetractStreamTableSink 

Defines an external TableSink to emit a streaming table with insert, update, and delete changes.

定义一个外部TableSink以发出具有插入，更新和删除更改的流表。

The interface looks as follows:

该接口如下所示：

```text
RetractStreamTableSink<T> implements TableSink<Tuple2<Boolean, T>> {
  public TypeInformation<T> getRecordType();
  public void emitDataStream(DataStream<Tuple2<Boolean, T>> dataStream);
}
```

The table will be converted into a stream of accumulate and retraction messages which are encoded as Java Tuple2. 
The first field is a boolean flag to indicate the message type (true indicates insert, false indicates delete). 
The second field holds the record of the requested type T.

该表将被转换为累积和撤消消息流，这些消息被编码为Java Tuple2。第一个字段是一个布尔型标志，指示消息类型（true指示插入，false指示删除）。第二个字段保存请求类型的记录T。


### UpsertStreamTableSink 

Defines an external TableSink to emit a streaming table with insert, update, and delete changes.

定义一个外部TableSink以发出具有插入，更新和删除更改的流表。

The interface looks as follows:

该接口如下所示：

```text
UpsertStreamTableSink<T> implements TableSink<Tuple2<Boolean, T>> {

  public void setKeyFields(String[] keys);

  public void setIsAppendOnly(boolean isAppendOnly);

  public TypeInformation<T> getRecordType();

  public void emitDataStream(DataStream<Tuple2<Boolean, T>> dataStream);
}
```

The table must be have unique key fields (atomic or composite) or be append-only. If the table does not have a unique key and is not append-only, a TableException will be thrown. 
The unique key of the table is configured by the UpsertStreamTableSink#setKeyFields() method.

该表必须具有唯一的键字段（原子键或复合键）或仅附加模式。如果表没有唯一键并且不是仅追加模式，TableException则将引发。该表的唯一键由UpsertStreamTableSink#setKeyFields()方法配置。

The table will be converted into a stream of upsert and delete messages which are encoded as a Java Tuple2. The first field is a boolean flag to indicate the message type. 
The second field holds the record of the requested type T.

该表将被转换为编码为Java Tuple2类型的upsert和delete消息流。第一个字段是指示消息类型的布尔标志。第二个字段保存请求类型的记录T。

A message with true boolean field is an upsert message for the configured key. A message with false flag is a delete message for the configured key. 
If the table is append-only, all messages will have a true flag and must be interpreted as insertions.

boolean字段为true的消息是一个配置了key的upsert消息。boolean字段为false的消息是一个配置了key的delete消息。 如果表是仅追加的，则所有消息都将具有true标志，并且必须将其解释为insert类型。

## Define a TableFactory 

A TableFactory allows to create different table-related instances from string-based properties. All available factories are called for matching to the given set of properties and a corresponding factory class.

TableFactory允许根据基于字符串的属性创建与表相关的不同实例。调用所有可用的工厂以匹配给定的属性集和相应的工厂类。

Factories leverage Java’s Service Provider Interfaces (SPI) for discovering. 
This means that every dependency and JAR file should contain a file org.apache.flink.table.factories.TableFactory in the META_INF/services resource directory that lists all available table factories that it provides.

工厂利用Java的服务提供商接口（SPI）进行发现。
这意味着每个依赖项和JAR文件都应org.apache.flink.table.factories.TableFactory在META_INF/services资源目录中包含一个文件，该文件列出了它提供的所有可用表工厂。

Every table factory needs to implement the following interface: 

每个表工厂都需要实现以下接口：

```text
package org.apache.flink.table.factories;

interface TableFactory {

  Map<String, String> requiredContext();

  List<String> supportedProperties();
}
```

- requiredContext(): Specifies the context that this factory has been implemented for. The framework guarantees to only match for this factory if the specified set of properties and values are met. 
Typical properties might be connector.type, format.type, or update-mode. Property keys such as connector.property-version and format.property-version are reserved for future backwards compatibility cases.

requiredContext()：指定已为此工厂实现的上下文。该框架保证仅在满足指定的属性和值集的情况下才与此工厂匹配。典型属性可能是connector.type，format.type或update-mode。
像connector.property-version和format.property-version等属性键保留给以后的向后兼容情况。

- supportedProperties(): List of property keys that this factory can handle. This method will be used for validation. If a property is passed that this factory cannot handle, an exception will be thrown. 
The list must not contain the keys that are specified by the context.

supportedProperties()：此工厂可以处理的属性键的列表。此方法将用于验证。如果传递了该工厂无法处理的属性，则将引发异常。该列表不得包含上下文指定的键。


In order to create a specific instance, a factory class can implement one or more interfaces provided in org.apache.flink.table.factories:

为了创建特定实例，工厂类可以实现一个或多个在org.apache.flink.table.factories中提供的接口：

BatchTableSourceFactory: Creates a batch table source. -- 创建一个批处理表源。 
BatchTableSinkFactory: Creates a batch table sink. -- 创建一个批处理表接收器。 
StreamTableSourceFactory: Creates a stream table source. -- 创建流表源。 
StreamTableSinkFactory: Creates a stream table sink. -- 创建一个流表接收器 
DeserializationSchemaFactory: Creates a deserialization schema format. -- 创建反序列化schema格式
SerializationSchemaFactory: Creates a serialization schema format. -- 创建序列化schmea格式  


The discovery of a factory happens in multiple stages:

工厂的discovery分为多个阶段： 

Discover all available factories. -- 发现所有可用的工厂 
Filter by factory class (e.g., StreamTableSourceFactory). -- 按工厂类别（例如StreamTableSourceFactory）过滤。 
Filter by matching context. -- 通过匹配上下文进行过滤 
Filter by supported properties. -- 按支持的属性过滤 
Verify that exactly one factory matches, otherwise throw an AmbiguousTableFactoryException or NoMatchingTableFactoryException. -- 验证一个工厂是否完全匹配，否则抛出AmbiguousTableFactoryException或NoMatchingTableFactoryException。


The following example shows how to provide a custom streaming source with an additional connector.debug property flag for parameterization.

下面的示例演示如何为自定义流源提供附加的connector.debug属性标志以进行参数化。 

```text
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class MySystemTableSourceFactory implements StreamTableSourceFactory<Row> {

  @Override
  public Map<String, String> requiredContext() {
    Map<String, String> context = new HashMap<>();
    context.put("update-mode", "append");
    context.put("connector.type", "my-system");
    return context;
  }

  @Override
  public List<String> supportedProperties() {
    List<String> list = new ArrayList<>();
    list.add("connector.debug");
    return list;
  }

  @Override
  public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
    boolean isDebug = Boolean.valueOf(properties.get("connector.debug"));

    # additional validation of the passed properties can also happen here

    return new MySystemAppendTableSource(isDebug);
  }
}
```

### Use a TableFactory in the SQL Client 

In a SQL Client environment file, the previously presented factory could be declared as: 

在SQL Client环境文件中，先前提供的工厂可以声明为：

```text
tables:
 - name: MySystemTable
   type: source
   update-mode: append
   connector:
     type: my-system
     debug: true
```

The YAML file is translated into flattened string properties and a table factory is called with those properties that describe the connection to the external system:

将YAML文件转换为扁平化的字符串属性，并使用描述与外部系统的连接的那些属性来调用表工厂：

```text
update-mode=append
connector.type=my-system
connector.debug=true
```

Attention : Properties such as tables.#.name or tables.#.type are SQL Client specifics and are not passed to any factory. 
The type property decides, depending on the execution environment, whether a BatchTableSourceFactory/StreamTableSourceFactory (for source), a BatchTableSinkFactory/StreamTableSinkFactory (for sink), 
or both (for both) need to discovered.

注意: 属性（例如tables.#.name或是tables.#.type SQL Client的特定属性）不会传递给任何工厂。
该type属性根据执行环境决定是否需要发现一个BatchTableSourceFactory/ StreamTableSourceFactory（用于source），
一个BatchTableSinkFactory/ StreamTableSinkFactory（用于sink），或者两者都both被发现。

### Use a TableFactory in the Table & SQL API 

For a type-safe, programmatic approach with explanatory Scaladoc/Javadoc, the Table & SQL API offers descriptors in org.apache.flink.table.descriptors that translate into string-based properties. 
See the built-in descriptors for sources, sinks, and formats as a reference.

对于使用说明性Scaladoc / Javadoc的类型安全的编程方法，Table＆SQL API提供了描述在org.apache.flink.table.descriptors，这些描述可转换为基于字符串的属性。请参阅源，接收器和格式的内置描述符作为参考。

A custom descriptor can be defined by extending the ConnectorDescriptor class. 

可以通过扩展ConnectorDescriptor类来定义自定义描述符。 

```text
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import java.util.HashMap;
import java.util.Map;

/**
  * Connector to MySystem with debug mode.
  */
public class MySystemConnector extends ConnectorDescriptor {

  public final boolean isDebug;

  public MySystemConnector(boolean isDebug) {
    super("my-system", 1, false);
    this.isDebug = isDebug;
  }

  @Override
  protected Map<String, String> toConnectorProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put("connector.debug", Boolean.toString(isDebug));
    return properties;
  }
}
```

The descriptor can then be used to create a table with the table environment. 

然后可以使用描述符在表环境中创建表。 

```text
StreamTableEnvironment tableEnv = // ...

tableEnv
  .connect(new MySystemConnector(true))
  .withSchema(...)
  .inAppendMode()
  .createTemporaryTable("MySystemTable");
```

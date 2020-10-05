
# Catalogs 

Catalogs provide metadata, such as databases, tables, partitions, views, and functions and information needed to access data stored in a database or other external systems.

One of the most crucial aspects of data processing is managing metadata. It may be transient metadata like temporary tables, or UDFs registered against the table environment. 
Or permanent metadata, like that in a Hive Metastore. Catalogs provide a unified API for managing metadata and making it accessible from the Table API and SQL Queries.

Catalog提供元数据，例如数据库，表，分区，视图以及访问存储在数据库或其他外部系统中的数据所需的功能和信息。

数据处理的最关键功能之一是管理元数据。它可能是临时元数据，例如临时表，或者是针对表环境注册的UDF。或永久性元数据，例如Hive Metastore中的元数据。Catalog提供了一个统一的API，用于管理元数据并使其可从Table API和SQL查询访问。

## Catalog Types

### GenericInMemoryCatalog

The GenericInMemoryCatalog is an in-memory implementation of a catalog. All objects will be available only for the lifetime of the session.

GenericInMemoryCatalog是一个基于内存的catalog。所有对象仅在会话的生存期内可用。

### HiveCatalog

The HiveCatalog serves two purposes; as persistent storage for pure Flink metadata, and as an interface for reading and writing existing Hive metadata. 
Flink’s Hive documentation provides full details on setting up the catalog and interfacing with an existing Hive installation.

HiveCatalog服务于两个目的; 作为纯Flink元数据的持久存储，以及作为读写现有Hive元数据的接口。Flink的Hive文档提供了有关设置catalog以及与现有Hive安装接口的完整详细信息。

Warning The Hive Metastore stores all meta-object names in lower case. This is unlike GenericInMemoryCatalog which is case-sensitive 

警告：Hive Metastore以小写形式存储所有元对象名称。这与GenericInMemoryCatalog区分大小写不同

### User-Defined Catalog

Catalogs are pluggable and users can develop custom catalogs by implementing the Catalog interface. To use custom catalogs in SQL CLI, 
users should develop both a catalog and its corresponding catalog factory by implementing the CatalogFactory interface.

Catalog是可插入的，用户可以通过实现Catalog接口来开发自定义Catalog服务。要在SQL CLI中使用自定义目录，用户应通过实现CatalogFactory接口来开发目录及其相应的目录工厂。

The catalog factory defines a set of properties for configuring the catalog when the SQL CLI bootstraps. 
The set of properties will be passed to a discovery service where the service tries to match the properties to a CatalogFactory and initiate a corresponding catalog instance.

目录工厂定义了一组属性，用于在SQL CLI引导时配置目录。属性集将传递给发现服务，在该服务中，服务尝试将这些属性与CatalogFactory匹配，并启动相应的catalog实例。

## How to Create and Register Flink Tables to Catalog 

### Using SQL DDL 

Users can use SQL DDL to create tables in catalogs in both Table API and SQL.

用户可以使用SQL DDL在Table API和SQL中在catalog中创建表。

For Table API:

```java
TableEnvironment tableEnv = ...

// Create a HiveCatalog 
Catalog catalog = new HiveCatalog("myhive", null, "<path_of_hive_conf>", "<hive_version>");

// Register the catalog
tableEnv.registerCatalog("myhive", catalog);

// Create a catalog database
tableEnv.sqlUpdate("CREATE DATABASE mydb WITH (...)");

// Create a catalog table
tableEnv.sqlUpdate("CREATE TABLE mytable (name STRING, age INT) WITH (...)");

tableEnv.listTables(); // should return the tables in current catalog and database.
```

For SQL Client:

```dbn-sql
// the catalog should have been registered via yaml file
Flink SQL> CREATE DATABASE mydb WITH (...);

Flink SQL> CREATE TABLE mytable (name STRING, age INT) WITH (...);

Flink SQL> SHOW TABLES;
mytable
```

For detailed information, please check out Flink SQL CREATE DDL.

### Using Java/Scala/Python API 

Users can use Java, Scala, or Python API to create catalog tables programmatically. 

用户可以使用Java，Scala或Python API以编程方式创建目录表。

```java
TableEnvironment tableEnv = ...

// Create a HiveCatalog
Catalog catalog = new HiveCatalog("myhive", null, "<path_of_hive_conf>", "<hive_version>");

// Register the catalog
tableEnv.registerCatalog("myhive", catalog);

// Create a catalog database
catalog.createDatabase("mydb", new CatalogDatabaseImpl(...))

// Create a catalog table
TableSchema schema = TableSchema.builder()
    .field("name", DataTypes.STRING())
    .field("age", DataTypes.INT())
    .build();

catalog.createTable(
        new ObjectPath("mydb", "mytable"),
        new CatalogTableImpl(
            schema,
            new Kafka()
                .version("0.11")
                ....
                .startFromEarlist(),
            "my comment"
        )
    );

List<String> tables = catalog.listTables("mydb"); // tables should contain "mytable"
```

## Catalog API 

Note: only catalog program APIs are listed here. Users can achieve many of the same funtionalities with SQL DDL. For detailed DDL information, please refer to SQL CREATE DDL. 
注意：此处仅列出catalog程序API。用户可以使用SQL DDL实现许多相同的功能。有关详细的DDL信息，请参阅SQL CREATE DDL。

### Database operations 

```java
// create database
catalog.createDatabase("mydb", new CatalogDatabaseImpl(...), false);

// drop database
catalog.dropDatabase("mydb", false);

// alter database
catalog.alterDatabase("mydb", new CatalogDatabaseImpl(...), false);

// get databse
catalog.getDatabase("mydb");

// check if a database exist
catalog.databaseExists("mydb");

// list databases in a catalog
catalog.listDatabases("mycatalog");
```

### Table operations

```java
// create table
catalog.createTable(new ObjectPath("mydb", "mytable"), new CatalogTableImpl(...), false);

// drop table
catalog.dropTable(new ObjectPath("mydb", "mytable"), false);

// alter table
catalog.alterTable(new ObjectPath("mydb", "mytable"), new CatalogTableImpl(...), false);

// rename table
catalog.renameTable(new ObjectPath("mydb", "mytable"), "my_new_table");

// get table
catalog.getTable("mytable");

// check if a table exist or not
catalog.tableExists("mytable");

// list tables in a database
catalog.listTables("mydb");
```

### View operations 

```java
// create view
catalog.createTable(new ObjectPath("mydb", "myview"), new CatalogViewImpl(...), false);

// drop view
catalog.dropTable(new ObjectPath("mydb", "myview"), false);

// alter view
catalog.alterTable(new ObjectPath("mydb", "mytable"), new CatalogViewImpl(...), false);

// rename view
catalog.renameTable(new ObjectPath("mydb", "myview"), "my_new_view");

// get view
catalog.getTable("myview");

// check if a view exist or not
catalog.tableExists("mytable");

// list views in a database
catalog.listViews("mydb");
```

### Partition operations 

```java
// create view
catalog.createPartition(
    new ObjectPath("mydb", "mytable"),
    new CatalogPartitionSpec(...),
    new CatalogPartitionImpl(...),
    false);

// drop partition
catalog.dropPartition(new ObjectPath("mydb", "mytable"), new CatalogPartitionSpec(...), false);

// alter partition
catalog.alterPartition(
    new ObjectPath("mydb", "mytable"),
    new CatalogPartitionSpec(...),
    new CatalogPartitionImpl(...),
    false);

// get partition
catalog.getPartition(new ObjectPath("mydb", "mytable"), new CatalogPartitionSpec(...));

// check if a partition exist or not
catalog.partitionExists(new ObjectPath("mydb", "mytable"), new CatalogPartitionSpec(...));

// list partitions of a table
catalog.listPartitions(new ObjectPath("mydb", "mytable"));

// list partitions of a table under a give partition spec
catalog.listPartitions(new ObjectPath("mydb", "mytable"), new CatalogPartitionSpec(...));

// list partitions of a table by expression filter
catalog.listPartitions(new ObjectPath("mydb", "mytable"), Arrays.asList(epr1, ...));
```

### Function operations 

```java
// create function
catalog.createFunction(new ObjectPath("mydb", "myfunc"), new CatalogFunctionImpl(...), false);

// drop function
catalog.dropFunction(new ObjectPath("mydb", "myfunc"), false);

// alter function
catalog.alterFunction(new ObjectPath("mydb", "myfunc"), new CatalogFunctionImpl(...), false);

// get function
catalog.getFunction("myfunc");

// check if a function exist or not
catalog.functionExists("myfunc");

// list functions in a database
catalog.listFunctions("mydb");
```

## Table API and SQL for Catalog 

### Registering a Catalog 

Users have access to a default in-memory catalog named default_catalog, that is always created by default. This catalog by default has a single database called default_database. 
Users can also register additional catalogs into an existing Flink session.

java : 
```java
tableEnv.registerCatalog(new CustomCatalog("myCatalog"));
```

yaml : 

All catalogs defined using YAML must provide a type property that specifies the type of catalog. The following types are supported out of the box.

使用YAML定义的所有目录都必须提供一个type指定目录类型的属性。开箱即用地支持以下类型。

|   Catalog Type                               |  Value             | 

| :------------------------------------------- | :----------------- |

|   GenericInMemory                            |  generic_in_memory |

|   Hive                                       |  hive              |

```yaml
catalogs:
   - name: myCatalog
     type: custom_catalog
     hive-conf-dir: ...
```

### Changing the Current Catalog And Database 

Flink will always search for tables, views, and UDF’s in the current catalog and database. 

java : 
```java
tableEnv.useCatalog("myCatalog");
tableEnv.useDatabase("myDb");
```

sql : 

```dbn-sql
Flink SQL> USE CATALOG myCatalog;
Flink SQL> USE myDB;
```

Metadata from catalogs that are not the current catalog are accessible by providing fully qualified names in the form catalog.database.object.

java : 
```java
tableEnv.from("not_the_current_catalog.not_the_current_db.my_table");
```

sql : 
```dbn-sql
Flink SQL> SELECT * FROM not_the_current_catalog.not_the_current_db.my_table;
```

### List Available Catalogs 

java :

```java
tableEnv.listCatalogs();
```

sql : 

```dbn-sql
Flink SQL> show catalogs;
```

### List Available Databases 

java :
```java
tableEnv.listDatabases(); 
```

sql : 
```dbn-sql
Flink SQL> show databases;
```

### List Available Tables 

java : 
```java
tableEnv.listTables();
```

sql :
```dbn-sql
Flink SQL> show tables;
```

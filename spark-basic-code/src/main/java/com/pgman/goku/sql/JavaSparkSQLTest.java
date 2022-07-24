package com.pgman.goku.sql;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;

/**
 * 
 * 普通RDD转换为Dataframe方法 ：
 * 
 * Spark SQL 支持两种不同的方法用于转换已存在的 RDD 成为 Dataset：
 * 
 * 第一种方法是使用反射去推断一个包含指定的对象类型的 RDD 的Schema。在你的 Spark 应用程序中当你已知 Schema
 * 时这个基于方法的反射可以让你的代码更简洁。
 * 
 * 第二种用于创建 Dataset 的方法是通过一个允许你构造一个 Schema 然后把它应用到一个已存在的 RDD的编程接口。
 * 然而这种方法更繁琐，当列和它们的类型知道运行时都是未知时它允许你去构造 Dataset。
 * 
 * @author ChavinKing
 *
 */
public class JavaSparkSQLTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// createDatasets();
		// rdd2DatasetsReflection();
		// rdd2DatasetsProgrammatically();
		// dataSourcesLoadAndSaveOperations();
		// hiveTables();
//		jdbcDataSources();

		sparkSQLDebug();
	}


	/**
	 * Spark SQL debug 任务
	 */
	public static void sparkSQLDebug() {

		String warehouseLocation = "/Users/chavinking/test/spark-warehouse";
		SparkSession spark = SparkSession.builder().master("local").appName("rdd2dfReflection")
				.config("spark.sql.warehouse.dir", warehouseLocation)
				.getOrCreate();

		Dataset<Row> people = spark.read().json("/Users/chavinking/github/goku-base/spark-basic-code/src/main/files/people.json");

//		people.printSchema();

		people.createOrReplaceTempView("people");

		Dataset<Row> namesDF = spark.sql("SELECT name,age FROM people");
		namesDF.show();

		spark.close();

	}




	/**
	 * 创建 Datasets:
	 * 
	 * Dataset 与 RDD 相似，然而，并不是使用 Java 序列化或者 Kryo，他们使用一个指定的 * Encoder（编码器）
	 * 来序列化用于处理或者通过网络进行传输的对象。虽然编码器和标准的序列化都负责将一个对象序列化成字节，编码器是动态生成的代码，并且使用了一种允许
	 * Spark 去执行许多像 filtering，sorting 以及 hashing 这样的操作，不需要将字节反序列化成对象的格式。
	 * 
	 */
	public static void createDatasets() {

		String warehouseLocation = "D://github//spark-study-project//spark-warehouse";
		SparkSession spark = SparkSession.builder().master("local").appName("rdd2dfReflection")
				.config("spark.sql.warehouse.dir", warehouseLocation)
				// .enableHiveSupport()
				.getOrCreate();

		// Create an instance of a Bean class
		Person person = new Person();
		person.setName("Andy");
		person.setAge(32);

		// Encoders are created for Java beans
		Dataset<Person> javaBeanDS = spark.createDataset(Collections.singletonList(person),
				Encoders.bean(Person.class));
		javaBeanDS.show();

		// Encoders for most common types are provided in class Encoders
		Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), Encoders.INT());
		Dataset<Integer> transformedDS = primitiveDS.map(new MapFunction<Integer, Integer>() {
			public Integer call(Integer value) throws Exception {
				return value + 1;
			}
		}, Encoders.INT());
		transformedDS.collect(); // Returns [2, 3, 4]

		// DataFrames can be converted to a Dataset by providing a class.
		// Mapping based on name
		String path = "files//people.json";
		Dataset<Person> peopleDS = spark.read().json(path).as(Encoders.bean(Person.class));
		peopleDS.show();

	}

	/**
	 * Inferring the Schema Using Reflection：使用反射推断 Schema
	 * 
	 * 当不能预先定义JavaBean类时(例如，记录的结构编码在字符串中，或者解析文本数据集，并且针对不同的用户以不同的方式投影字段)，
	 * 可以通过三个步骤以编程方式创建数据集<Row>。
	 * 
	 * 使用反射获得的BeanInfo定义了表的模式。目前，Spark
	 * SQL不支持包含映射字段的javabean。但是支持嵌套的javabean和列表或数组字段。
	 * 您可以通过创建一个实现Serializable的类来创建JavaBean，该类具有所有字段的getter和setter。
	 * 
	 */
	@SuppressWarnings("serial")
	public static void rdd2DatasetsReflection() {

		String warehouseLocation = "D://github//spark-study-project//spark-warehouse";
		SparkSession spark = SparkSession.builder().master("local").appName("rdd2dfReflection")
				.config("spark.sql.warehouse.dir", warehouseLocation)
				// .enableHiveSupport()
				.getOrCreate();

		// Create an RDD of Person objects from a text file
		JavaRDD<Person> peopleRDD = spark.read().textFile("files//people.txt").javaRDD()
				.map(new Function<String, Person>() {
					public Person call(String line) throws Exception {
						Person person = new Person();
						String[] parts = line.split(",");
						person.setName(parts[0]);
						person.setAge(Integer.parseInt(parts[1].trim()));
						return person;
					}
				});

		// Apply a schema to an RDD of JavaBeans to get a DataFrame
		Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
		// Register the DataFrame as a temporary view
		peopleDF.createOrReplaceTempView("people");

		// SQL statements can be run by using the sql methods provided by spark
		Dataset<Row> teenagersDF = spark.sql("SELECT name,age FROM people WHERE age <= 28");

		teenagersDF.show();

		teenagersDF.printSchema();

		// The columns of a row in the result can be accessed by field index
		Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(new MapFunction<Row, String>() {
			public String call(Row row) throws Exception {
				return "Name: " + row.getString(0) + ", Age: " + String.valueOf(row.getInt(1));
			}
		}, Encoders.STRING());
		teenagerNamesByIndexDF.show();

		// or by field name
		Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(new MapFunction<Row, String>() {
			public String call(Row row) throws Exception {
				return "Name1: " + row.getAs("name") + ", Age1: " + row.getAs("age");
			}
		}, Encoders.STRING());
		teenagerNamesByFieldDF.show();

		spark.close();
	}

	/**
	 * 
	 * Programmatically Specifying the Schema：以编程的方式指定 Schema
	 * 
	 * 当 case class不能够在执行之前被定义（例如，records 记录的结构在一个 string 字符串中被编码了，或者一个 text 文本
	 * dataset 将被解析并且不同的用户投影的字段是不一样的）。一个 DataFrame 可以使用下面的三步以编程的方式来创建。
	 * 
	 * 从原始的 RDD 创建 RDD 的 RowS（行）。
	 * 
	 * Step 1 被创建后，创建 Schema 表示一个 StructType 匹配 RDD中的 Rows（行）的结构。
	 * 
	 * 通过 SparkSession 提供的 createDataFrame 方法应用 Schema 到 RDD 的RowS（行）。
	 * 
	 */
	public static void rdd2DatasetsProgrammatically() {

		String warehouseLocation = "D://github//spark-study-project//spark-warehouse";
		SparkSession spark = SparkSession.builder().master("local").appName("rdd2dfReflection")
				.config("spark.sql.warehouse.dir", warehouseLocation)
				// .enableHiveSupport()
				.getOrCreate();

		// Create an RDD
		JavaRDD<String> peopleRDD = spark.sparkContext().textFile("files//people.txt", 1).toJavaRDD();

		// The schema is encoded in a string
		String schemaString = "name age";

		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName : schemaString.split(" ")) {
			StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);

		// Convert records of the RDD (people) to Rows
		JavaRDD<Row> rowRDD = peopleRDD.map(new Function<String, Row>() {
			public Row call(String record) throws Exception {
				String[] attributes = record.split(",");
				return RowFactory.create(attributes[0], attributes[1].trim());
			}
		});

		// Apply the schema to the RDD
		Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

		// Creates a temporary view using the DataFrame
		peopleDataFrame.createOrReplaceTempView("people");

		// SQL can be run over a temporary view created using DataFrames
		Dataset<Row> results = spark.sql("SELECT name,age FROM people WHERE age >= 28");

		// The results of SQL queries are DataFrames and support all the normal
		// RDD operations
		// The columns of a row in the result can be accessed by field index or
		// by field name
		Dataset<String> namesDS = results.map(new MapFunction<Row, String>() {
			public String call(Row row) throws Exception {
				return "Name: " + row.getString(0) + ", Age: " + row.getString(1);
			}
		}, Encoders.STRING());
		namesDS.show();

		spark.close();
	}

	/**
	 * 
	 * 通用的 Load/Save 函数:
	 * 
	 * 在最简单的方式下，默认的数据源（parquet 除非另外配置通过spark.sql.sources.default）将会用于所有的操作。
	 * 
	 * 手动指定选项:
	 * 
	 * 你也可以手动的指定数据源，并且将与你想要传递给数据源的任何额外选项一起使用。 数据源由其完全限定名指定（例如 :
	 * org.apache.spark.sql.parquet），不过对于内置数据源你也可以使用它们的缩写名（json, parquet, jdbc）。
	 * 使用下面这个语法可以将从任意类型数据源加载的DataFrames 转换为其他类型。
	 * 
	 * 直接在文件上运行 SQL:
	 * 
	 * 你也可以直接在文件上运行 SQL 查询来替代使用 API 将文件加载到 DataFrame 再进行查询。
	 * 
	 */
	public static void dataSourcesLoadAndSaveOperations() {

		String warehouseLocation = "D://github//spark-study-project//spark-warehouse";
		SparkSession spark = SparkSession.builder().master("local").appName("rdd2dfReflection")
				.config("spark.sql.warehouse.dir", warehouseLocation)
				// .enableHiveSupport()
				.getOrCreate();

		// 通用的 Load/Save 函数:
		Dataset<Row> usersDF = spark.read().load("files//users.parquet");
		usersDF.select("name", "favorite_color").write().save("files//namesAndFavColors.parquet");

		// 手动指定选项:
		Dataset<Row> peopleDF = spark.read().format("json").load("files//people.json");
		peopleDF.select("name", "age").write().format("parquet").save("namesAndAges.parquet");

		// 直接在文件上运行 SQL:
		Dataset<Row> sqlDF = spark.sql("SELECT * FROM parquet.`files//users.parquet`");

		spark.close();

	}

	/**
	 * 
	 * Parquet 文件
	 * 
	 */
	public static void parquetFilesLoadOperations() {

		String warehouseLocation = "D://github//spark-study-project//spark-warehouse";
		SparkSession spark = SparkSession.builder().master("local").appName("rdd2dfReflection")
				.config("spark.sql.warehouse.dir", warehouseLocation)
				// .enableHiveSupport()
				.getOrCreate();

		Dataset<Row> peopleDF = spark.read().json("files//people.json");

		// DataFrames can be saved as Parquet files, maintaining the schema
		// information
		peopleDF.write().parquet("people.parquet");

		// Read in the Parquet file created above.
		// Parquet files are self-describing so the schema is preserved
		// The result of loading a parquet file is also a DataFrame
		Dataset<Row> parquetFileDF = spark.read().parquet("people.parquet");

		// Parquet files can also be used to create a temporary view and then
		// used in SQL statements
		parquetFileDF.createOrReplaceTempView("parquetFile");
		Dataset<Row> namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19");
		Dataset<String> namesDS = namesDF.map(new MapFunction<Row, String>() {
			public String call(Row row) {
				return "Name: " + row.getString(0);
			}
		}, Encoders.STRING());
		namesDS.show();

		spark.close();

	}

	/**
	 * 
	 * Schema 合并:
	 * 
	 * 类似 ProtocolBuffer，Avro，以及 Thrift，Parquet 也支持 schema 演变。 用户可以从一个简单的 schema
	 * 开始，并且根据需要逐渐地向 schema 中添加更多的列。 这样，用户最终可能会有多个不同但是具有相互兼容 schema 的 Parquet
	 * 文件。 Parquet 数据源现在可以自动地发现这种情况，并且将所有这些文件的 schema 进行合并。
	 * 
	 */
	public static void parquetFilesMergeOperations() {

		String warehouseLocation = "D://github//spark-study-project//spark-warehouse";
		SparkSession spark = SparkSession.builder().master("local").appName("rdd2dfReflection")
				.config("spark.sql.warehouse.dir", warehouseLocation)
				// .enableHiveSupport()
				.getOrCreate();

		List<Square> squares = new ArrayList<Square>();
		for (int value = 1; value <= 5; value++) {
			Square square = new Square();
			square.setValue(value);
			square.setSquare(value * value);
			squares.add(square);
		}

		// Create a simple DataFrame, store into a partition directory
		Dataset<Row> squaresDF = spark.createDataFrame(squares, Square.class);
		squaresDF.write().parquet("data/test_table/key=1");

		List<Cube> cubes = new ArrayList<Cube>();
		for (int value = 6; value <= 10; value++) {
			Cube cube = new Cube();
			cube.setValue(value);
			cube.setCube(value * value * value);
			cubes.add(cube);
		}

		// Create another DataFrame in a new partition directory,
		// adding a new column and dropping an existing column
		Dataset<Row> cubesDF = spark.createDataFrame(cubes, Cube.class);
		cubesDF.write().parquet("data/test_table/key=2");

		// Read the partitioned table
		Dataset<Row> mergedDF = spark.read().option("mergeSchema", true).parquet("data/test_table");
		mergedDF.printSchema();

		spark.close();

	}

	/**
	 * 
	 * Spark SQL 可以自动的推断出 JSON 数据集的 schema 并且将它作为 DataFrame 进行加载。 这个转换可以通过使用
	 * SparkSession.read.json() 在字符串类型的 RDD 中或者 JSON 文件。
	 * 
	 */
	public static void jsonDatasets() {

		String warehouseLocation = "D://github//spark-study-project//spark-warehouse";
		SparkSession spark = SparkSession.builder().master("local").appName("rdd2dfReflection")
				.config("spark.sql.warehouse.dir", warehouseLocation)
				// .enableHiveSupport()
				.getOrCreate();

		// A JSON dataset is pointed to by path.
		// The path can be either a single text file or a directory storing text
		// files
		Dataset<Row> people = spark.read().json("files//people.json");

		// The inferred schema can be visualized using the printSchema() method
		people.printSchema();

		// Creates a temporary view using the DataFrame
		people.createOrReplaceTempView("people");

		// SQL statements can be run by using the sql methods provided by spark
		Dataset<Row> namesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");
		namesDF.show();

		// Alternatively, a DataFrame can be created for a JSON dataset
		// represented by
		// an RDD[String] storing one JSON object per string.
		List<String> jsonData = Arrays
				.asList("{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
		JavaRDD<String> anotherPeopleRDD = new JavaSparkContext(spark.sparkContext()).parallelize(jsonData);
		Dataset anotherPeople = spark.read().json(anotherPeopleRDD);
		anotherPeople.show();

		spark.close();

	}

	/**
	 * 
	 * Hive TABLES:
	 * 
	 * Spark SQL 还支持在 Apache Hive 中读写数据。然而，由于 Hive 依赖项太多，这些依赖没有包含在默认的 Spark
	 * 发行版本中。如果在 classpath 上配置了 Hive 依赖，那么 Spark 会自动加载它们。注意，Hive 依赖也必须放到所有的
	 * worker 节点上，因为如果要访问 Hive 中的数据它们需要访问 Hive 序列化和反序列化库（SerDes)。
	 * 
	 * Hive 配置是通过将 hive-site.xml，core-site.xml（用于安全配置）以及 hdfs-site.xml（用于HDFS
	 * 配置）文件放置在conf/目录下来完成的。
	 * 
	 * 如果要使用 Hive, 你必须要实例化一个支持 Hive 的 SparkSession，包括连接到一个持久化的 Hive metastore,
	 * 支持 Hive 序列化反序列化库以及 Hive 用户自定义函数。即使用户没有安装部署 Hive 也仍然可以启用 Hive 支持。如果没有在
	 * hive-site.xml 文件中配置，Spark 应用程序启动之后，上下文会自动在当前目录下创建一个 metastore_db 目录并创建一个由
	 * spark.sql.warehouse.dir 配置的、默认值是当前目录下的 spark-warehouse 目录的目录。请注意 : 从
	 * Spark 2.0.0 版本开始，hive-site.xml 中的 hive.metastore.warehouse.dir
	 * 属性就已经过时了，你可以使用 spark.sql.warehouse.dir 来指定仓库中数据库的默认存储位置。你可能还需要给启动 Spark
	 * 应用程序的用户赋予写权限。
	 * 
	 */
	public static void hiveTables() {

		String warehouseLocation = "D://github//spark-study-project//spark-warehouse";
		SparkSession spark = SparkSession.builder().master("local").appName("rdd2dfReflection")
				.config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate();

		spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)");
		spark.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src");

		// Queries are expressed in HiveQL
		spark.sql("SELECT * FROM src").show();

		// Aggregation queries are also supported.
		spark.sql("SELECT COUNT(*) FROM src").show();

		// The results of SQL queries are themselves DataFrames and support all
		// normal functions.
		Dataset<Row> sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key");

		// The items in DaraFrames are of type Row, which lets you to access
		// each column by ordinal.
		Dataset<String> stringsDS = sqlDF.map(new MapFunction<Row, String>() {
			public String call(Row row) throws Exception {
				return "Key: " + row.get(0) + ", Value: " + row.get(1);
			}
		}, Encoders.STRING());
		stringsDS.show();

		// You can also use DataFrames to create temporary views within a
		// SparkSession.
		List<Record> records = new ArrayList<Record>();
		for (int key = 1; key < 100; key++) {
			Record record = new Record();
			record.setKey(key);
			record.setValue("val_" + key);
			records.add(record);
		}
		Dataset<Row> recordsDF = spark.createDataFrame(records, Record.class);
		recordsDF.createOrReplaceTempView("records");

		// Queries can then join DataFrames data with data stored in Hive.
		spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show();

		spark.close();

	}

	/**
	 * 
	 * Spark SQL 还有一个能够使用 JDBC 从其他数据库读取数据的数据源。当使用 JDBC 访问其它数据库时，应该首选
	 * JdbcRDD。这是因为结果是以数据框（DataFrame）返回的，且这样 Spark SQL操作轻松或便于连接其它数据源。因为这种 JDBC
	 * 数据源不需要用户提供 ClassTag，所以它也更适合使用 Java 或 Python 操作。
	 * 
	 * @author ChavinKing
	 *
	 */

	public static void jdbcDataSources() {

		String warehouseLocation = "D://github//spark-study-project//spark-warehouse";
		SparkSession spark = SparkSession.builder().master("local").appName("rdd2dfReflection")
				.config("spark.sql.warehouse.dir", warehouseLocation)
				// .enableHiveSupport()
				.getOrCreate();

		// Note: JDBC loading and saving can be achieved via either the
		// load/save or jdbc methods
		// Loading data from a JDBC source
		Dataset<Row> jdbcDF = spark.read().format("jdbc").option("url", "jdbc:mysql://192.168.72.1:3306/bd_dim")
				.option("dbtable", "bd_dim.dict_transcoding").option("user", "root").option("password", "mysql").load();

		jdbcDF.show();

		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "root");
		connectionProperties.put("password", "mysql");
		Dataset<Row> jdbcDF2 = spark.read().jdbc("jdbc:mysql://192.168.72.1:3306/bd_dim", "bd_dim.dict_transcoding",
				connectionProperties);

		jdbcDF2.show();

		// Saving data to a JDBC source
		jdbcDF.write().format("jdbc").option("url", "jdbc:mysql://192.168.72.1:3306/bd_dim")
				.option("dbtable", "bd_dim.dict_transcoding_save_01").option("user", "root").option("password", "mysql")
				.mode("append").save();

		jdbcDF2.write().mode("append").jdbc("jdbc:mysql://192.168.72.1:3306/bd_dim", "bd_dim.dict_transcoding_save_02",
				connectionProperties);

		spark.close();

	}

	public static class Person implements Serializable {
		private String name;
		private int age;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}
	}

	public static class Square implements Serializable {
		private int value;
		private int square;

		public int getValue() {
			return value;
		}

		public void setValue(int value) {
			this.value = value;
		}

		public int getSquare() {
			return square;
		}

		public void setSquare(int square) {
			this.square = square;
		}

		// Getters and setters...

	}

	public static class Cube implements Serializable {
		private int value;
		private int cube;

		public int getValue() {
			return value;
		}

		public void setValue(int value) {
			this.value = value;
		}

		public int getCube() {
			return cube;
		}

		public void setCube(int cube) {
			this.cube = cube;
		}

		// Getters and setters...

	}

	public static class Record implements Serializable {
		private int key;
		private String value;

		public int getKey() {
			return key;
		}

		public void setKey(int key) {
			this.key = key;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}
	}

}

## 阅读记录

项目构建
```
mvn archetype:generate                \
  -DarchetypeGroupId=org.apache.flink   \
  -DarchetypeArtifactId=flink-quickstart-java \
  -DarchetypeVersion=1.16.0
```

官网（1.16）已完成内容如下：
- Flink开发
- 内幕
- try flink 
- 实战练习
- 概念透析
- 应用开发
  - 项目配置
  - datastream api
    - 数据类型及其序列化（非）



## 知识点

1 slot是flink分配资源的最小单位，每个slot可以运行多个task组成的流任务。

2 task是flink管理任务的最小执行单元，每个task都运行在不同的slot中，不同task可以运行在一个slot中。

3 一个 Flink 集群总是包含一个 JobManager 以及一个或多个 Flink TaskManager。JobManager 负责处理 Job 提交、 Job 监控以及资源管理。Flink TaskManager 运行 worker 进程， 负责实际任务 Tasks 的执行，而这些任务共同组成了一个 Flink Job。

4 wartmark

- 1-定义时间戳
- 2-生成水位线



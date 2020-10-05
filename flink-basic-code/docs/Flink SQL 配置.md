
# 配置


By default, the Table & SQL API is preconfigured for producing accurate results with acceptable performance.

Depending on the requirements of a table program, it might be necessary to adjust certain parameters for optimization. 
For example, unbounded streaming programs may need to ensure that the required state size is capped (see streaming concepts).

默认情况下，Table＆SQL API已预先配置为产生具有可接受性能的准确结果。

根据表程序的要求，可能需要调整某些参数以进行优化。例如，无界流程序可能需要确保所需的状态大小是有上限的（请参阅流概念）。

## Overview 

In every table environment, the TableConfig offers options for configuring the current session.

在每个表环境中，TableConfig提供了用于配置当前会话的选项。

For common or important configuration options, the TableConfig provides getters and setters methods with detailed inline documentation.

对于常见或重要的配置选项，TableConfig提供了具有详细内联文档的getter和setter方法。

For more advanced configuration, users can directly access the underlying key-value map. The following sections list all available options that can be used to adjust Flink Table & SQL API programs.

对于更高级的配置，用户可以直接访问基础键值映射。以下各节列出了可用于调整Flink Table和SQL API程序的所有可用选项。

Attention Because options are read at different point in time when performing operations, it is recommended to set configuration options early after instantiating a table environment.

注意 : 由于执行操作时会在不同的时间点读取选项，因此建议在实例化表环境后尽早设置配置选项。

```text
// instantiate table environment
TableEnvironment tEnv = ...

// access flink configuration
Configuration configuration = tEnv.getConfig().getConfiguration();
// set low-level key-value options
configuration.setString("table.exec.mini-batch.enabled", "true");
configuration.setString("table.exec.mini-batch.allow-latency", "5 s");
configuration.setString("table.exec.mini-batch.size", "5000");
```

Attention Currently, key-value options are only supported for the Blink planner.  -- 当前，仅blink planner支持键值选项。

## Execution Options 

The following options can be used to tune the performance of the query execution. -- 以下选项可用于调整查询执行的性能。 

参考 ： https://ci.apache.org/projects/flink/flink-docs-release-1.10/zh/dev/table/config.html#execution-options 






















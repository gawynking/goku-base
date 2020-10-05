package com.pgman.goku.datastream.state;


/**
 * Flink状态：
 *      flink中状态分为2种：
 *          键控状态(Keyed State)
 *          算子状态(Operator State)
 *      flink管理状态的2种方式：Raw State 与 Managed State
 *          托管状态（Raw State）：托管状态由Flink运行时控制的数据结构表示，如：内部哈希表或RocksDB。
 *          原始状态（Managed State）：原始状态是操作符保存在自己的数据结构中的状态。当检查点时，它们只将字节序列写入检查点。Flink对状态的数据结构一无所知，只看到原始字节。
 *          所有数据流功能都可以使用托管状态，但原始状态接口只能在实现操作符时使用。建议使用托管状态（而不是原始状态），因为在托管状态下，
 *          Flink能够在并行性更改时自动重新分配状态，并且还可以进行更好的内存管理。
 *
 *      flink中Managed Keyed State状态的分类：
 *          1 ValueState<T> ：这保留了一个可以更新和检索的值（如上所述，作用于输入元素的键的范围，因此操作看到的每个键可能有一个值）。可以使用update(T)设置该值，并使用T value()检索该值。
 *          2 ListState<T>：这将保留元素列表。可以追加元素并在所有当前存储的元素上检索Iterable。使用add(T)或addAll(List<T>)添加元素，
 *          可以使用Iterable<T> get()检索Iterable。您还可以使用update(List<T>)覆盖现有列表
 *          3 ReducingState<T>：这保留一个值，表示添加到状态的所有值的聚合。该接口类似于ListState，但使用add(T)添加的元素使用指定的ReduceFunction缩减为聚合。
 *          4 AggregatingState<IN, OUT>：这保留一个值，表示添加到状态的所有值的聚合。与ReducingState相反，聚合类型可能与添加到状态的元素类型不同。接口与ListState相同，
 *          但使用add(IN)添加的元素使用指定的AggregateFunction进行聚合。
 *          5 FoldingState<T, ACC>：这保留一个值，表示添加到状态的所有值的聚合。与ReducingState相反，聚合类型可能与添加到状态的元素类型不同。该接口类似于ListState，
 *          但使用add(T)添加的元素使用指定的FoldFunction折叠为聚合。
 *          6 MapState<UK, UV>：这将保留映射列表。您可以将键值对放入状态，并在所有当前存储的映射上检索Iterable。使用put(UK, UV)或者putAll(Map<UK, UV>)添加映射。
 *          可以使用get(UK)检索与用户密钥关联的值。可以分别使用entries()，keys()和values()来检索映射，键和值的可迭代视图。
 *
 *      Flink中应用算子的三种方式：
 *          1 RichFunction应用状态编程
 *          2 processFunction应用状态编程
 *          3 xxxWithState算子应用状态编程
 *
 *      状态有效期 (TTL)：
 *          任何类型的 keyed state 都可以有 有效期 (TTL)。如果配置了 TTL 且状态值已过期，则会尽最大可能清除对应的值，所有状态类型都支持单元素的 TTL。 这意味着列表元素和映射元素将独立到期。
 *
 */
public class BaseStateExample {

    public static void main(String[] args) {

        String groupId = "base-state-order";



    }





}

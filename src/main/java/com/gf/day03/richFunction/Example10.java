package com.gf.day03.richFunction;

import com.gf.utils.IntegerSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 *  todo
 *      ValueState:
 *          值状态变量
 *          ValueState<T>：这保留了一个可以更新和检索的值（范围为上面提到的输入元素的键，因此操作看到的每个键都可能有一个值）。
 *          可以使用 设置update(T)和检索该值T value()。
 *  todo
 *      ListState<T>：这保留了一个元素列表。您可以附加元素并检索Iterable 所有当前存储的元素。add(T)使用或添加元素addAll(List<T>)，
 *          可以使用 检索 Iterable Iterable<T> get()。您还可以使用覆盖现有列表update(List<T>)
 *      ReducingState<T>：这会保留一个值，该值表示添加到状态的所有值的聚合。接口类似于，ListState但添加的元素 add(T)使用指定的ReduceFunction.
 *      AggregatingState<IN, OUT>：这会保留一个值，该值表示添加到状态的所有值的聚合。
 *          与 相反ReducingState，聚合类型可能与添加到状态的元素类型不同。该接口与 for 相同，ListState但使用添加的元素使用add(IN)指定的 聚合AggregateFunction。
 *      MapState<UK, UV>：这会保留一个映射列表。您可以将键值对放入状态并检索Iterable所有当前存储的映射。使用put(UK, UV)或 添加映射putAll(Map<UK, UV>)。
 *          可以使用 检索与用户键关联的值get(UK)。映射、键和值的可迭代视图可以分别使用entries()和检索。您还可以使用来检查此映射是否包含任何键值映射。keys()values()isEmpty()
 *
 * todo
 *  所有类型的状态也有一个方法clear()可以清除当前活动键的状态，即输入元素的键。
 *  重要的是要记住，这些状态对象仅用于与状态交互。状态不一定存储在内部，但可能驻留在磁盘或其他地方。
 *  要记住的第二件事是，您从状态中获得的值取决于输入元素的键。因此，如果涉及的键不同，您在一次用户函数调用中获得的值可能与另一次调用中的值不同。
 *  要获得状态句柄，您必须创建一个StateDescriptor. 这包含状态的名称（正如我们稍后将看到的，您可以创建多个状态，并且它们必须具有唯一的名称以便您可以引用它们），
 *  状态保存的值的类型，以及可能的用户 -指定的函数，例如ReduceFunction. 根据您要检索的状态类型，
 *  您可以创建  ValueStateDescriptor、  ListStateDescriptor、 AggregatingStateDescriptor、 ReducingStateDescriptor或  MapStateDescriptor。
 *  状态是使用 访问的RuntimeContext，所以只能在丰富的函数中使用。请参阅此处了解相关信息，但我们很快也会看到一个示例。RuntimeContext中可用的具有RichFunction以下访问状态的方法：
 *  ValueState<T> getState(ValueStateDescriptor<T>)
 *  ReducingState<T> getReducingState(ReducingStateDescriptor<T>)
 *  ListState<T> getListState(ListStateDescriptor<T>)
 *  AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT>)
 *  MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV>)
 *
 */
public class Example10 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env
                .addSource(new IntegerSource())
                .keyBy(r->1)
                .process(new KeyedProcessFunction<Integer, Integer, IntegerStatistic>() {
                    /**
                     * todo
                     *    声明状态变量
                     *    每个key维护自己的状态变量
                     *    状态变量是一个单例，只能初始化一次
                     */
                    private ValueState<IntegerStatistic> accmulator;//并行子任务里面的全局变量(其他并行子任务是不可见的)

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        //初始化了一个空累加器
                        accmulator=getRuntimeContext().getState(
                                //字符串acc表示状态变量 在状态后端里面的名字
                                new ValueStateDescriptor<IntegerStatistic>("acc", Types.POJO(IntegerStatistic.class))
                        );
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                    }

                    @Override
                    public void processElement(Integer value, Context ctx, Collector<IntegerStatistic> out) throws Exception {
                        // .value() 用来读取值状态变量中的值
                        if (accmulator.value()==null) {
                            //第一条数据到达
                            // .update更新状态变量
                            accmulator.update(
                                    new IntegerStatistic(
                                            value,
                                            value,
                                            value,
                                            1,
                                            value
                                    )
                            );
                        }else {
                            //输入数据和累加器进行聚合并更新累加器,这里的tmp就是累加器
                            IntegerStatistic tmp = accmulator.value();
                            //这里的newAcc是新的数据进来后和累加器聚合得到新的数据
                            IntegerStatistic newAcc = new IntegerStatistic(
                                    Math.max(value, tmp.max),
                                    Math.min(value, tmp.min),
                                    value, tmp.sum,
                                    (value + tmp.sum) / (tmp.count + 1)
                            );
                            accmulator.update(newAcc);
                        }
                        out.collect(accmulator.value());
                    }
                })
                .print();


        env.execute();
    }


    public static class IntegerStatistic{
        public Integer max;
        public Integer min;
        public Integer sum;
        public Integer count;
        public Integer avg;

        public IntegerStatistic() {
        }

        public IntegerStatistic(Integer max, Integer min, Integer sum, Integer count, Integer avg) {
            this.max = max;
            this.min = min;
            this.sum = sum;
            this.count = count;
            this.avg = avg;
        }

        @Override
        public String toString() {
            return "IntegerStatistic{" +
                    "max=" + max +
                    ", min=" + min +
                    ", sum=" + sum +
                    ", count=" + count +
                    ", avg=" + avg +
                    '}';
        }
    }

}

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
 *      定时输出统计值，比如隔10秒钟输出一次统计值
 *      限流
 */
public class FlinkValueState_Pojo_onTimer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new IntegerSource())
                /*
                    keyBy不是一个算子，因为没有进行计算
                    只是为数据指定key,并将数据路由到对应逻辑分区
                 */
                .keyBy(r->1)
                //process方法占用一个任务插槽
                //process方法在一个线程运行
                .process(new KeyedProcessFunction<Integer, Integer, IntegerStatistic>() {
                    /**
                     * todo
                     *    声明状态变量
                     *    每个key维护自己的状态变量
                     *    状态变量是一个单例，只能初始化一次
                     */
                    private ValueState<IntegerStatistic> accmulator;

                    //用来保存定时器时间戳
                    private ValueState<Long> timerTs;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        //初始化了一个空累加器
                        /**
                         * todo
                         *    {
                         *      1: IntStatistic
                         *    }
                         */
                        accmulator=getRuntimeContext().getState(
                                //字符串acc表示状态变量 在状态后端里面的名字
                                new ValueStateDescriptor<IntegerStatistic>("acc", Types.POJO(IntegerStatistic.class))
                        );
                        /**
                         * todo
                         *   {
                         *    1: Long
                         *   }
                         */
                        timerTs =getRuntimeContext().getState(
                                new ValueStateDescriptor<Long>("timer-ts",Types.LONG)
                        );
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                    }

                    //原子性
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
                        //todo 注意:accmulator需要维护历史IntegerStatistic数据，不清空

                        //第一条数据到来时，timerTs为空
                        //onTimer执行之后，timerTs为空
                        if (timerTs.value()==null) {
                            //如果不存在定时器，那么注册一个10秒钟之后的定时器
                            long currTs = ctx.timerService().currentProcessingTime();
                            long tenSecLater = currTs + 10 * 1000L;
                            //注册发送统计数据的定时器
                            /**
                             * todo
                             *    定时器队列的哈希表
                             *      {
                             *        1: [30s,40s]
                             *      }
                             */
                            ctx.timerService().registerProcessingTimeTimer(tenSecLater);
                            //保存定时器时间戳
                            /**
                             * todo
                             *      timerTs
                             *        {
                             *          1: 30*1000
                             *        }
                             */
                            timerTs.update(tenSecLater);
                        }
                    }

                    //定时器用来向下游发送统计数据
                    //只有定时器触发执行，不能注册新的定时器
                    //OnTimer只能有一个，多个定时器，在这个函数中写if,else
                    //具有原子性
                    //由于processElement和onTimer都可以操作状态变量，所以必须保证原子性
                    //定时器触发执行，将从定时器队列中弹出
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<IntegerStatistic> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        //向下游发送统计数据
                        out.collect(accmulator.value());
                        //清空保存定时器时间戳的状态变量
                        //清空操作只清空该key对应的value值，key不会被清除
                        timerTs.clear();
                        /*
                          todo
                            定时器队列的哈希表
                                {
                                  1: []
                                }
                         */
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

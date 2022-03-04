package com.gf.day03.richFunction;

import com.gf.utils.ClickEvent;
import com.gf.utils.ClickSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class FlinkValueStatePojo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .keyBy(r->1)
                .process(
                        new KeyedProcessFunction<Integer, ClickEvent, ClickEvent>() {

                            //状态累加器
                            private ValueState<ClickEvent> accmulator;

                            //定时器
                            private ValueState<Long> timerTs;

                            //初始化累加器
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                accmulator= getRuntimeContext().getState(
                                        //初始状态获取状态变量
                                        new ValueStateDescriptor<ClickEvent>("acc", Types.POJO(ClickEvent.class))
                                );
                                timerTs= getRuntimeContext().getState(
                                        new ValueStateDescriptor<Long>("timer-ts" ,Types.LONG));
                            }

                            @Override
                            public void processElement(ClickEvent value, Context ctx, Collector<ClickEvent> out) throws Exception {
                                //数据第一次到达
                                if (accmulator.value()==null) {
                                    accmulator.update(
                                           new ClickEvent(" "," ",0L)
                                    );
                                }else {
                                    //不是第一次到达
                                    ClickEvent tmp = accmulator.value();
                                    accmulator.update(tmp);
                                }
                                accmulator.update(value);
                                //定时器第一次
                                if (timerTs.value()==null) {
                                    long currTs = ctx.timerService().currentProcessingTime();
                                    long tenSecLater = currTs + 3 * 1000L;
                                    //注册定时器在onTimer运行
                                    ctx.timerService().registerProcessingTimeTimer(tenSecLater);
                                    timerTs.update(tenSecLater);
                                }
                            }
                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<ClickEvent> out) throws Exception {
                                super.onTimer(timestamp, ctx, out);
                                //向下游发送数据
                                out.collect(accmulator.value());
                                //清空定时器
                                timerTs.clear();
                            }
                        }
                )
                .print();

        env.execute();
    }
}

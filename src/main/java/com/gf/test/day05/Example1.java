package com.gf.test.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env     //数据: a,1(事件时间是1秒)
                .socketTextStream("localhost",9999)
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        return Tuple2.of(arr[0],
                                Long.parseLong(arr[1])*1000L); //数据变为 a,1000L
                    }
                })
                .assignTimestampsAndWatermarks(
                        //最大延迟时间为5s
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String,Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                //告诉flink，事件时间的字段是哪一个
                                return element.f1;
                            }
                        })
                )
                .keyBy(r->r.f0)//对不同的数据进行分区
                //对时间开窗，窗口大小是5
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    private ValueState<Long> stateFlag;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        stateFlag= getRuntimeContext().getState(
                                new ValueStateDescriptor<Long>(
                                        "acc", Types.LONG
                                )
                        );
                    }
                    //process 相当于onTimer
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        out.collect("窗口"+context.window().getStart()+"~"+context.window().getEnd()
                        +"数据总数为"+elements.spliterator().getExactSizeIfKnown()+"当前的水位线为："+context.currentWatermark());
                    }

                })
                .print();

        env.execute();
    }
}

package com.gf.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

//水位线测试
public class FlinkWatermarks_KeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                //a 1,事件时间是1秒,
                .socketTextStream("localhost",9999)
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split(" ");

                        return Tuple2.of(
                                arr[0],
                                Long.parseLong(arr[1])*1000L); //转成时间戳
                    }
                })
                /**
                 *  在map输出的数据流中插入水位线
                 *    默认每隔200毫秒的机器时间插入一次水位线
                 *    每次插入水位线时，插入多大的水位线呢
                 *     水位线= 观察到最大时间戳-最大延迟时间-1毫秒
                 */
                .assignTimestampsAndWatermarks(
                        //设置最大延迟时间是5秒钟，forBoundedOutOfOrderness 需要设置数据流中数据泛型
                        WatermarkStrategy.<Tuple2<String,Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                //告诉flink，f1字段是事件时间戳字段
                                return element.f1;
                            }
                        })
                )
                .keyBy(r->r.f0)//Tuple2<String,Long>
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        out.collect("当前数据"+value+"当前逻辑时钟(水位线)是:"+ctx.timerService().currentWatermark()
                                +"当前注册的时间为："+new Timestamp(value.f1+9999L)+"定时事件");
                        ctx.timerService().registerEventTimeTimer(value.f1+9999L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        out.collect("定时器触发了，触发的数据是:"+ctx.getCurrentKey()+"上一个触发的时间是:"+new Timestamp(timestamp));

                    }
                })

                .print();
        env.execute();
    }
}

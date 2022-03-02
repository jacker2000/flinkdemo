package com.gf.test.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("localhost",9999)
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        return Tuple2.of(
                                arr[0],
                                Long.parseLong(arr[1])*1000L
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        //最大延迟时间设置为5秒
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String,Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        })
                )
                .process(new ProcessFunction<Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        out.collect("到达的数据是:"+value+"当前process逻辑时钟是:"+new Timestamp(ctx.timerService().currentWatermark())+
                                "当前的处理时间为:"+new Timestamp(ctx.timerService().currentProcessingTime()));
                    }

                             @Override
                             public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                                 super.onTimer(timestamp, ctx, out);
                                 //链式ontimer处理方式
                             }
                         }
                )

                .print();
        env.execute();
    }
}

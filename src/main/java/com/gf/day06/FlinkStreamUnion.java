package com.gf.day06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 *  允许流的数据类型不同
 */
public class FlinkStreamUnion {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Long>> stream1 = env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        return Tuple2.of(
                                arr[0],
                                Long.parseLong(arr[1]) * 1000L
                        );
                    }
                })
                //设置水位线
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                })
                );
        SingleOutputStreamOperator<Tuple2<String, Long>> stream2 = env
                .socketTextStream("localhost", 9998)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        return Tuple2.of(
                                arr[0],
                                Long.parseLong(arr[1]) * 1000L
                        );
                    }
                })
                //设置水位线
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                })
                );
        /**
         * 双流union的时候，选择水位线最小的那个和下游进行同步
         */
        stream1.union(stream2)
                .process(
                        new ProcessFunction<Tuple2<String, Long>, String>() {
                            @Override
                            public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                                out.collect("数据为:"+value+"到达，当前算子的逻辑时钟:"+ctx.timerService().currentWatermark());
                            }
                        }
                ).print();

        env.execute();
    }
}

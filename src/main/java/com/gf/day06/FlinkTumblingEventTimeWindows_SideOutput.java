package com.gf.day06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.time.Duration;

public class FlinkTumblingEventTimeWindows_SideOutput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<String> result = env
                .socketTextStream("localhost", 9999)
                .map(r -> Tuple2.of(r.split(" ")[0], Long.parseLong(r.split(" ")[1]) * 1000L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                //分配水位线
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                })
                )
                .keyBy(r -> r.f0)
                //滚动窗口5秒
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(5))
                .sideOutputLateData(new OutputTag<Tuple2<String, Long>>("late") {
                })
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        StringBuilder builder = new StringBuilder();
                        for (Tuple2<String, Long> element : elements) {
                            builder.append(element.f0 + ",");
                        }

                        out.collect("当前逻辑时钟(水位线)是:" + context.currentWatermark() +
                                "当前的处理时间为：" + new Timestamp(context.currentProcessingTime()) +
                                "数据为:" + builder.toString() + "key是:" + context.window().getStart() + "~" +
                                context.window().getEnd() + "共有:" +
                                elements.spliterator().getExactSizeIfKnown() +
                                "数据,由process算子的并行子任务索引是:" +
                                getRuntimeContext().getIndexOfThisSubtask());
                    }
                });
        result.print();
        result.getSideOutput(new OutputTag<Tuple2<String, Long>>("late") {
        }).print("侧输出流的数据为:");
        env.execute();
    }
}

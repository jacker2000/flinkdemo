package com.gf.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

//水位线测试
public class FlinkSetWatermarkInterval {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(60*1000L);
                //a 1,事件时间是1秒,
                env.socketTextStream("localhost",9999)
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
                        WatermarkStrategy.<Tuple2<String,Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        //告诉flink，f1字段是事件时间戳字段
                                        return element.f1;
                                    }
                                })
                )
                .keyBy(r->r.f0)
                //事件窗口大小
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        out.collect("当前逻辑时钟(水位线)是:"+context.currentWatermark()
                                +"当前的处理时间为："+new Timestamp(context.currentProcessingTime())
                                +"窗口"+context.window().getStart()+"~"+context.window().getEnd()
                                +"数据总数为"+elements.spliterator().getExactSizeIfKnown());
                    }
                })
                .print();
        env.execute();
    }
}

package com.gf.test.day05;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Example6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .socketTextStream("localhost",9999)
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        return Tuple2.of(arr[0],Long.parseLong(arr[1])*1000L);
                    }
                })
                //每隔200ms机器时间插入一次水位线
                .assignTimestampsAndWatermarks(
                        //最大延迟时间设置5秒钟，水位线的底层实现
                        new WatermarkStrategy<Tuple2<String, Long>>() {
                            @Override
                            public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                                return new WatermarkGenerator<Tuple2<String, Long>>() {
                                    private long bound = 5000L; //最大延迟时间是5000毫秒
                                    private long maxTs =Long.MIN_VALUE+bound+1L;
                                    @Override
                                    public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
                                        //每来一条事件触发一次调用
                                        maxTs=Math.max(maxTs,event.f1);
                                    }

                                    @Override
                                    public void onPeriodicEmit(WatermarkOutput output) {
                                        //默认200ms调用一次
                                        output.emitWatermark(new Watermark(maxTs-bound-1L));
                                    }
                                };
                            }

                            @Override
                            public TimestampAssigner<Tuple2<String, Long>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                                return new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                };
                            }
                        }
                )
                .keyBy(r->r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {

                        StringBuilder builder = new StringBuilder();
                        for (Tuple2<String, Long> element : elements) {
                            builder.append(element.f0+",");
                        }
                        String result = builder.toString().substring(0, builder.length() - 1);
                        out.collect("窗口的key值:"+s+"值数据为："+result+"窗口范围:"+context.window().getStart()+"~"+context.window().getEnd()+"总浏览数："
                                +elements.spliterator().getExactSizeIfKnown());
                    }
                }).print();


        env.execute();
    }
}

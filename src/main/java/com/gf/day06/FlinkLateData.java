package com.gf.day06;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;

public class FlinkLateData {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> result = env
                //添加数据源
                .addSource(new SourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        ctx.collectWithTimestamp(1, 1000L);
                        Thread.sleep(1000L);
                        ctx.collectWithTimestamp(2, 2000L);
                        Thread.sleep(1000L);
                        ctx.emitWatermark( new Watermark(5000L));
                        Thread.sleep(1000L);
                        ctx.collectWithTimestamp(3, 3000L);
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .process(new ProcessFunction<Integer, String>() {
                    @Override
                    public void processElement(Integer value, Context ctx, Collector<String> out) throws Exception {
                        if (ctx.timestamp() < ctx.timerService().currentWatermark()) {
                            ctx.output(
                                    //侧输出流的名字，单例
                                    //泛型表示侧输出流中事件类型
                                    new OutputTag<String>("late") {
                                    },
                                    "迟到元素:" + value + "发送到侧输出流中去"+"事件时间为:"+new Timestamp(ctx.timestamp())+
                                    "数据到达时间"+new Timestamp(ctx.timerService().currentProcessingTime())
                            );
                        } else {
                            out.collect("元素:" + value + "没有迟到"+"事件时间为:"+new Timestamp(ctx.timestamp())+
                                    "数据到达时间"+new Timestamp(ctx.timerService().currentProcessingTime()));
                        }
                    }
                });
        result.print();
        result.getSideOutput(new OutputTag<String>("late"){}).print();

        env.execute();
    }
}

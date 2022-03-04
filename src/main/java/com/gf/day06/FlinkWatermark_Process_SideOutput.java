package com.gf.day06;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class FlinkWatermark_Process_SideOutput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<String> result = env
                .addSource(new SourceFunction<String>() {
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        ctx.collectWithTimestamp("hello", 1000L);
                        ctx.collectWithTimestamp("hello", 3000L);
                        ctx.emitWatermark(new Watermark(5000L));
                        ctx.collectWithTimestamp("hello", 7000L);
                        ctx.collectWithTimestamp("hello", 2000L);
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        //当前水位线>时间戳,则当前数据是迟到数据
                        if (ctx.timerService().currentWatermark() > ctx.timestamp()) {
                            ctx.output(new OutputTag<String>("late-data"){},
                                    "数据"+value+"迟到了，发送的测输出流，事件时间:"+ctx.timestamp());
                        } else {
                            out.collect("数据" + value + "没迟到，事件时间是:" + ctx.timestamp());
                        }
                    }
                });
        result.print("主流数据:");
        result.getSideOutput(new OutputTag<String>("late-data") {}).print("测输出流:");
        env.execute();
    }
}

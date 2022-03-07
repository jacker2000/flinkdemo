package com.gf.day09;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

//超时未支付订单监测

public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple3<String, String, Long>> stream = env.addSource(new SourceFunction<Tuple3<String, String, Long>>() {
            @Override
            public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {
                ctx.collectWithTimestamp(Tuple3.of("order-1", "create", 1000L), 1000L);
                Thread.sleep(1000L);
                ctx.collectWithTimestamp(Tuple3.of("order-2", "create", 2000L), 2000L);
                Thread.sleep(1000L);
                ctx.collectWithTimestamp(Tuple3.of("order-2", "create", 4000L), 4000L);
                Thread.sleep(1000L);
                ctx.emitWatermark(new Watermark(7000L));
                Thread.sleep(1000L);
                ctx.collectWithTimestamp(Tuple3.of("order-1", "pay", 9000L), 9000L);
                Thread.sleep(1000L);
            }

            @Override
            public void cancel() {

            }
        });
        Pattern<Tuple3<String, String, Long>, Tuple3<String, String, Long>> pattern = Pattern
                .<Tuple3<String, String, Long>>begin("first")
                .where(new SimpleCondition<Tuple3<String, String, Long>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, Long> value) throws Exception {
                        return value.f1.equals("create");
                    }
                })
                .next("second")
                .where(new SimpleCondition<Tuple3<String, String, Long>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, Long> value) throws Exception {
                        return value.f1.equals("pay");
                    }
                })
                //要求5秒之内匹配到两个事情，否则超时
                .within(Time.seconds(5));
        PatternStream<Tuple3<String, String, Long>> patternStream = CEP.pattern(stream.keyBy(r -> r.f0), pattern);
        SingleOutputStreamOperator<String> result = patternStream.flatSelect(
                //测输出流用来接收超时信息
                new OutputTag<String>("timeout") {
                },
                //处理超时信息匿名类
                new PatternFlatTimeoutFunction<Tuple3<String, String, Long>, String>() {
                    @Override
                    public void timeout(Map<String, List<Tuple3<String, String, Long>>> pattern, long l, Collector<String> out) throws Exception {
                        Tuple3<String, String, Long> create = pattern.get("first").get(0);
                        //out.collect 将数据发送到测输出流
                        out.collect("订单" + create.f0 + "超时未支付");
                    }
                },
                //处理正常支付的信息
                new PatternFlatSelectFunction<Tuple3<String, String, Long>, String>() {
                    @Override
                    public void flatSelect(Map<String, List<Tuple3<String, String, Long>>> pattern, Collector<String> out) throws Exception {
                        Tuple3<String, String, Long> create = pattern.get("first").get(0);
                        Tuple3<String, String, Long> pay = pattern.get("second").get(0);
                        out.collect("订单" + create.f0 + "正常支付,支付的时间是:" + pay.f2);
                    }
                }
        );
        result.print();
        result.getSideOutput(new OutputTag<String>("timeout") {}).print("测输出流");
        env.execute();
    }
}

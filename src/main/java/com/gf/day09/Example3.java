package com.gf.day09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * 连续3次登录失败监测
 *
 */
public class Example3 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> loginStream = env.fromElements(
                Tuple3.of("user-1", "fail", 1000L),
                Tuple3.of("user-1", "fail", 2000L),
                Tuple3.of("user-2", "success", 3000L),
                Tuple3.of("user-1", "fail", 4000L),
                Tuple3.of("user-1", "fail", 5000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        })
        );
        //定义监测模板
        Pattern<Tuple3<String, String, Long>, Tuple3<String, String, Long>> parStream = Pattern
                .<Tuple3<String, String, Long>>begin("login-fail")
                .where(new SimpleCondition<Tuple3<String, String, Long>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, Long> value) throws Exception {
                        return value.f1.equals("fail");
                    }
                })
                .times(3)      //匹配三个事件
                .consecutive(); //三个事件紧挨着



        //在流上匹配模板
        PatternStream<Tuple3<String, String, Long>> patternStream = CEP.pattern(loginStream.keyBy(r -> r.f0), parStream);

        patternStream
                .flatSelect(new PatternFlatSelectFunction<Tuple3<String, String, Long>, String>() {
                    @Override
                    public void flatSelect(Map<String, List<Tuple3<String, String, Long>>> pattern, Collector<String> out) throws Exception {
                        //login-fail => [Event,Event,Event]
                        Tuple3<String, String, Long> first = pattern.get("login-fail").get(0);
                        Tuple3<String, String, Long> second = pattern.get("login-fail").get(0);
                        Tuple3<String, String, Long> third = pattern.get("login-fail").get(0);
                        out.collect("用户:"+first.f0+"在时间戳:"+first.f2+";"+second.f2+";"+
                                third.f2+"登录失败");
                    }
                }).print();
        env.execute();
    }
}

package com.gf.test.day03;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(
                        Tuple2.of(Tuple2.of(1,2), Tuple2.of(4,5)),
                        Tuple2.of(Tuple2.of(5,6), Tuple2.of(7,8))
                //输入，输入，累计器
                )
                .keyBy(new KeySelector<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>>() {
                    @Override
                    public Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> getKey(Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> value) throws Exception {
                        return value;
                    }
                })
                //todo 元组不支持加法
                .sum(0) //第一个元组字段 ==f1
                .print();

        env.execute();
    }
}

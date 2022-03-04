package com.gf.test.day03;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(
                        Tuple2.of(Tuple2.of(1,2), 3),
                        Tuple2.of(Tuple2.of(4,5), 6)

                )
                .keyBy(new KeySelector<Tuple2<Tuple2<Integer, Integer>, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> getKey(Tuple2<Tuple2<Integer, Integer>, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                //key是Tuple2<Integer, Integer>，如果想实现2个tuple2的相加，需要自己实现sum
                .sum(1)
                .print();

        env.execute();
    }
}

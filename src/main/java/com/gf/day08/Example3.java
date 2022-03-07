package com.gf.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.fromElements(1,2,3,4,5)
                .keyBy(r->r%2)
                /*
                1  1        1
                0  2        2
                1  3+1      4
                0  4+2      6
                1  5+4      9
                 */
                .sum(0)
                .print();
        env.execute();
    }
}

package com.gf.test.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example8 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(1,2,3,4,5)
                .keyBy(r->1)
                /*
                    sum里面的数据为索引位置，对于基础类型来说，索引位置都是0
                    tuple索引位置可以是1,2,3...
                    如果要聚合的是list,则位置也不一定
                 */
                .sum(0)
                .print();

        env.execute();
    }
}

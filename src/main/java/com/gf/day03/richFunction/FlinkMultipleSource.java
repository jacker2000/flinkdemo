package com.gf.day03.richFunction;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

//自定义并行数据源

public class FlinkMultipleSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                for (int i = 0; i < 9; i++) {
                    ctx.collect(i);
                }
            }
            @Override
            public void cancel() {

            }
        }).setParallelism(2)
          .print().setParallelism(2); //这样两个顶点会合成一个任务链去执行

        env.execute();
    }
}

package com.gf.day03.richFunction;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 *  多任务数据源
 */
public class FlinkRebalance {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new RichParallelSourceFunction<String>() {
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        for (int i = 0; i < 9; i++) {
                            if (i%2==getRuntimeContext().getIndexOfThisSubtask()) {
                                ctx.collect("并行子任务:"+getRuntimeContext().getIndexOfThisSubtask()+
                                        "发送数据:"+i);
                            }
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                }).setParallelism(2)
                .rebalance() //.rescale() 可替换
                .print().setParallelism(4);

        env.execute();
    }
}

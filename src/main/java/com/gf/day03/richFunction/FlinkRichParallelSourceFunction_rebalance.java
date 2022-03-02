package com.gf.day03.richFunction;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class FlinkRichParallelSourceFunction_rebalance {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env
                .addSource(new RichParallelSourceFunction<String>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        System.out.println("并行子任务的索引是:"+getRuntimeContext().getIndexOfThisSubtask()
                        +"生命周期开始");
                    }

                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        for (int i = 1; i < 9; i++) {
                            if (i%2==getRuntimeContext().getIndexOfThisSubtask()) {
                                ctx.collect("并行子任务索引是:"+getRuntimeContext().getIndexOfThisSubtask()+
                                        "发送的数据是:"+i);
                            }
                        }
                    }

                    @Override
                    public void cancel() {

                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        System.out.println("并行子任务的索引是:"+getRuntimeContext().getIndexOfThisSubtask()
                                +"生命周期结束");
                    }
                }).setParallelism(2)
                /**
                 *  todo
                 *      rebalance会把上游的0的发送到下游的每个分区即(1,2,3,4)
                 *      同样上游的1，也会发送到下游的每个分区即(1,2,3,4)
                 *
                 *      即每个数据源的并行子任务会将数据发送到print的所有任务槽
                 */
                .rebalance()
                .print().setParallelism(4);

        env.execute();
    }
}

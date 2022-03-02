package com.gf.day03.richFunction;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;


public class FlinkRichParallelSourceFunction_rescale {
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
                 *    下游如果是上游的倍数可以使用rescale,效率会高
                 *    上游的第一个会发送到下游的前两个，下游的第二个会发送到下游的后两个
                 *      如索引是0的，发送到1,2 ，索引是1的发送到3,4
                 *
                 *    数据源的并行子任务只会将数据发送到print的一部分任务槽中
                 */
                .rescale()
                .print().setParallelism(4);

        env.execute();
    }
}

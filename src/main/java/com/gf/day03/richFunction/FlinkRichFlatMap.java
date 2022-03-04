package com.gf.day03.richFunction;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *  富函数:
 *      RichFlatMapFunction
 */
public class FlinkRichFlatMap {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env
                .fromElements(1,2,3,4,5,6)
                .flatMap(new RichFlatMapFunction<Integer, String>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        System.out.println("并行子任务的索引:"+getRuntimeContext().getIndexOfThisSubtask());
                    }

                    @Override
                    public void flatMap(Integer value, Collector<String> out) throws Exception {
                        out.collect( "输入数据为:"+value+" 输出的值为:"+value*value+"当前并行子任务的索引为:"+getRuntimeContext().getIndexOfThisSubtask());
                    }
                }).setParallelism(2)
                .print().setParallelism(2);

        env.execute();
    }
}

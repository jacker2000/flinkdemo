package com.gf.day03.richFunction;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class FlinkProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env
                .fromElements("white","black","gray")
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        if ("white".equals(value)) {
                            out.collect(value);
                        }else if("black".equals(value)){
                            out.collect(value);
                        }
                    }
                })
                .print();

        env.execute();
    }
}

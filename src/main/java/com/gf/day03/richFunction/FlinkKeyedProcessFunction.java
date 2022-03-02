package com.gf.day03.richFunction;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class FlinkKeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env
                .socketTextStream("localhost",9999)
                .keyBy(r->1)
                .process(new KeyedProcessFunction<Integer, String, String>() {
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

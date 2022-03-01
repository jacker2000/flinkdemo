package com.gf.day03.richFunction;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @author gaofan
 * @date 2022/2/27 21:21
 * @Version 1.0
 * @description:
 *      定时器
 */

public class Example9 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("localhost",9999)
                .keyBy(r->1)
                .process(new KeyedProcessFunction<Integer, String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        //获取事件到达process算子机器时间
                        long currTs = ctx.timerService().currentProcessingTime();
                        out.collect("事件:"+value+"到达! 到达时间是"+new Timestamp(currTs));
                        //注册一个10s钟之后的定时器
                        ctx.timerService().registerProcessingTimeTimer(currTs+10*1000L);

                    }
                    //当时间进行到注册定时器的时间戳时，触发onTime的调用
                    //参数timestamp是定时器的时间戳
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        out.collect("定时器触发了！触发时间是:"+new Timestamp(timestamp));
                    }
                })
                .print();

        env.execute();
    }
}

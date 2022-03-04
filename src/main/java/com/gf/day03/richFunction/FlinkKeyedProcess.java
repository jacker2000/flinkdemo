package com.gf.day03.richFunction;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Time;
import java.sql.Timestamp;

public class FlinkKeyedProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("localhost",9999)
                .keyBy(r->1)
                .process(new KeyedProcessFunction<Integer, String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        //数据到达process 算子处理时间
                        long currentTs = ctx.timerService().currentProcessingTime();
                        //10s之后时间为
                        long tenSecondTs = currentTs + 10 * 1000L;
                        //注册定时器
                        ctx.timerService().registerProcessingTimeTimer(tenSecondTs);
                        out.collect("key为"+ctx.getCurrentKey()+"的数据"+value+"到达，时间为:"+
                                new Timestamp(currentTs)+"注册一个时间戳"+new Timestamp(tenSecondTs)+"的定时器");
                    }

                    //timestamp保存在定时器队列，内存中
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        //timestamp参数就是上面tenSecondTs
                        out.collect("定时器触发,触发时间为:"+new Time(timestamp));
                    }
                })
                .print();
        env.execute();
    }
}

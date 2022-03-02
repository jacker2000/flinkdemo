package com.gf.day05;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new SourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        //第一个会话窗口
                        ctx.collect(1);
                        Thread.sleep(1000L);
                        ctx.collect(1);
                        Thread.sleep(10*1000L);
                        //第二个会话窗口
                        ctx.collect(1);
                        Thread.sleep(10*1000L);
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .keyBy(r->1)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                .process(new ProcessWindowFunction<Integer, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, Context context, Iterable<Integer> elements, Collector<String> out) throws Exception {
                        out.collect("窗口"+new Timestamp(context.window().getStart())+"~"+
                        new Timestamp(context.window().getEnd())+"共有:"+
                    elements.spliterator().getExactSizeIfKnown()+"条数据");
                    }
                })
                .print();
        env.execute();
    }
}

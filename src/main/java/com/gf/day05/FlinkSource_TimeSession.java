package com.gf.day05;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class FlinkSource_TimeSession {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SourceFunction<String>() {
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        //第一个参数是向下游发送的事件，第二个参数是为这个事件指定的事件时间
                        ctx.collectWithTimestamp("hello",1000L);//6000毫秒结束时间,水位线999,
                        Thread.sleep(1000L);
                        ctx.collectWithTimestamp("hello",3000L);//8000毫秒结束时间,水位线2999

                        Thread.sleep(1000L);
                        //发送水位线,和process同步
                        ctx.emitWatermark(new Watermark(8000L));//水位线2999
                        Thread.sleep(1000L);
                        ctx.collectWithTimestamp("hello",4000L);//12000毫秒定时器,
                        //到达的数据的时间戳小于当前算子的水位线，就是迟到数据
                        Thread.sleep(1000L);
                        ctx.collectWithTimestamp("hello",9000L);//14000毫秒定时器
                        Thread.sleep(1000L);
                    }
                    @Override
                    public void cancel() {
                    }
                })
                .keyBy(r->1)
                //5秒会话窗口,窗口范围大小由会话窗口初始size+可见最大时间决定
                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                .process(new ProcessWindowFunction<String, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        out.collect("窗口"+context.window().getStart()+"~"
                        +context.window().getEnd()+"总数:"+elements.spliterator().getExactSizeIfKnown());
                    }
                })
                .print();

        env.execute();
    }
}

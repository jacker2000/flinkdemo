package com.gf.day05;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class FlinkEventTimeSessionWindows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new SourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        //第一个会话窗口
                        ctx.collectWithTimestamp(1,1000L);
                        Thread.sleep(1000L);
                        ctx.collectWithTimestamp(2,2000L);
                        Thread.sleep(10*1000L);
                        ctx.emitWatermark(new Watermark(7000L));
                        Thread.sleep(1000L);
                        //第二个会话窗口
                        ctx.collectWithTimestamp(3,3000L);
                        Thread.sleep(10*1000L);
                        ctx.collectWithTimestamp(8,8000L);
                        Thread.sleep(10*1000L);
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .keyBy(r->1)
                /*
                    todo:
                        事件会话窗口大小是根据: 窗口大小+发送事件戳决定，
                        当第一个会话窗口关闭后，第二个窗口会积攒会话数据等第二次同步process，直到全部处理结束
                 */
                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                .process(new ProcessWindowFunction<Integer, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, Context context, Iterable<Integer> elements, Collector<String> out) throws Exception {
                        StringBuilder builder = new StringBuilder();
                        for (Integer element : elements) {
                            builder.append(element+",");
                        }
                        out.collect("窗口数据为:"+builder.toString()+"窗口范围:"+new Timestamp(context.window().getStart())+"~"+
                                new Timestamp(context.window().getEnd())+"共有:"+
                                elements.spliterator().getExactSizeIfKnown()+"条数据");
                    }
                })
                .print();
        env.execute();
    }
}

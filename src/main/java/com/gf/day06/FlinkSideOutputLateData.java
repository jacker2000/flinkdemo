package com.gf.day06;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class FlinkSideOutputLateData {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<String> result = env
                //添加数据源
                .addSource(new SourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        ctx.collectWithTimestamp(1, 1000L);
                        Thread.sleep(1000L);
                        ctx.emitWatermark( new Watermark(999L));
                        Thread.sleep(1000L);
                        ctx.collectWithTimestamp(2, 2000L);
                        ctx.emitWatermark( new Watermark(1999L));
                        Thread.sleep(1000L);
                        ctx.collectWithTimestamp(15, 1500L);
                        Thread.sleep(1000L);
                        ctx.emitWatermark( new Watermark(5000L));
                        Thread.sleep(1000L);
                        ctx.collectWithTimestamp(3, 3000L);
                    }

                    @Override
                    public void cancel() {

                    }
                } )
                .keyBy(r->1)
                /**
                 *   todo:
                 *      滚动窗口大小:就是TumblingEventTimeWindows设置的事件
                 *      如果在窗口内的数据水位线都在窗口之内，即使同步到process但 水位线<窗口大小，也是不能触发第二个窗口执行的
                 *      则不触发第二个窗口执行,否则触发
                 *      水位线 ≥ 窗口结束时间 触发窗口执行
                 *      水位线 ≥ 窗口结束时间 + allowedLateness，（.allowedLateness(Time.seconds(5))）则会触发窗口销毁
                 */
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //将迟到且窗口销毁的元素发送到侧输出流中
                //侧输出流中元素类型必须和窗口中元素类型一致
                .sideOutputLateData(new OutputTag<Integer>("late"){})
                .process(new ProcessWindowFunction<Integer, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, Context context, Iterable<Integer> elements, Collector<String> out) throws Exception {
                        StringBuilder str = new StringBuilder();
                        for (Integer element : elements) {
                            str.append(element+",");
                        }
                        out.collect("窗口中共有:"+elements.spliterator().getExactSizeIfKnown()+"条数据,数据为:"+str.toString()+
                                "窗口范围:"+context.window().getStart()+"~"+context.window().getEnd());
                    }
                });
        result.print();
        result.getSideOutput(new OutputTag<Integer>("late"){}).print("侧输出流:");

        env.execute();
    }
}

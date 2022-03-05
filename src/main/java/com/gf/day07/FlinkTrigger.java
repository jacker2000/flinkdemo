package com.gf.day07;

import com.gf.utils.ClickEvent;
import com.gf.utils.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;


/**
 *  触发器Trigger
 */
public class FlinkTrigger {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ClickEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<ClickEvent>() {
                            @Override
                            public long extractTimestamp(ClickEvent element, long recordTimestamp) {
                                return element.ts;
                            }
                        })
                )
                .keyBy(r->r.url)
                .window(TumblingEventTimeWindows.of(Time.days(10)))
                //触发器触发的是窗口的计算，也就是aggregate的执行
        .trigger(
                new Trigger<ClickEvent, TimeWindow>() {
                    //每来一条数据执行一次
                    @Override
                    public TriggerResult onElement(ClickEvent element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        //窗口状态变量，每个窗口独有的
                        ValueState<Boolean> flag = ctx.getPartitionedState(new ValueStateDescriptor<Boolean>("flag", Types.BOOLEAN));

                        //if中的语句只想对窗口中的第一条数据执行
                        if (flag.value()==null) {
                            //当第一条数据到达时，注册一个接下来的整数秒的定时器
                            //9876毫秒
                            long nextSecond = element.ts + 1000L - element.ts % 1000L;
                            //注册定时器onEventTime
                            ctx.registerEventTimeTimer(nextSecond);
                            flag.update(true);
                        }
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return null;
                    }
                    //处理时间到达time时，触发执行
                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        //窗口结束时间>处理时间到达的time
                        if (window.getEnd()>time) {
                            if (window.getEnd()>time+1000L) {
                                //注册的还是onEventTime
                                ctx.registerEventTimeTimer(time+1000L);
                            }
                            return TriggerResult.FIRE; //触发窗口计算
                        }
                        return TriggerResult.CONTINUE;
                    }
                    //窗口关闭时，触发执行
                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
                        ValueState<Boolean> flag = ctx.getPartitionedState(new ValueStateDescriptor<Boolean>("flag", Types.BOOLEAN));
                        flag.clear();
                    }
                }
        ).aggregate(
                new AggregateFunction<ClickEvent, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(ClickEvent value, Long accumulator) {
                        return accumulator + 1L;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return null;
                    }
                },
                new ProcessWindowFunction<Long, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
                        out.collect(s+"在窗口"+new Timestamp(context.window().getStart())+"~"+
                                new Timestamp(context.window().getEnd())+"里面有:"+elements.iterator().next());
                    }
                }
        ).print();

        env.execute();
    }
}

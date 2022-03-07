package com.gf.day08;

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
                .keyBy(r -> 1)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .trigger(new MyTrigger())
                .aggregate(new AggCount(), new WindowResult())
                .print();


        env.execute();
    }
    //窗口中的第一条数据的时间戳后面的整数秒，都要触发一次窗口的计算
    //例如时间戳是1234ms的话，那么需要在2,3,4,5,6,7,8,9都要触发窗口的计算，但是不销毁窗口
    public static class MyTrigger extends Trigger<ClickEvent, TimeWindow>{
        //窗口中每来一条数据触发一次调用
        @Override
        public TriggerResult onElement(ClickEvent element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            //当前窗口私有的状态变量
            ValueState<Boolean> isFirstEvent = ctx.getPartitionedState(
                    new ValueStateDescriptor<Boolean>("is-first-enent", Types.BOOLEAN) //"is-first-enent", Types.BOOLEAN
            );
            if (isFirstEvent.value()==null) {
                //窗口中的第一条数据到达
                //nextSecond是element.ts后面的第一个整数秒
                long nextSecond = element.ts + 1000L - element.ts % 1000L;
                //注册的是onEventTime定时器
                ctx.registerEventTimeTimer(nextSecond);
                //状态变量设置为true,保证if分支只对第一条数据执行
                isFirstEvent.update(true);
            }
            /*
               CONTINUE      什么都不做
               FIRE_AND_PURGE 触发窗口计算，并且毁掉清空窗口
               FIRE  触发窗口计算，不清空窗口
               PURGE 不触发窗口计算 ，毁掉清空窗口
             */
            //窗口什么都不做
            return TriggerResult.CONTINUE; //什么都不做
        }

        //处理时间定时器
        //当处理时间到达参数time时，触发onProcessingTime调用
        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return null;
        }

        //事件时间定时器
        //当水位线到达参数time时，触发onEventTime的调用
        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            if (window.getEnd()>time) {
                if (time+1000L<window.getEnd()) {
                    //注册的还是onEventTime定时器，也就是说注册了本身
                    ctx.registerEventTimeTimer(time+1000L);
                }
                return TriggerResult.FIRE;//触发窗口的计算，也就是后面的.aggregate方法执行
            }
            return TriggerResult.CONTINUE;
        }

        //窗口闭合销毁的时候调用
        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
           //注意是单例
            ValueState<Boolean> isFirstEvent = ctx.getPartitionedState(
                    new ValueStateDescriptor<Boolean>("is-first-enent", Types.BOOLEAN) //"is-first-enent", Types.BOOLEAN
            );
            isFirstEvent.clear();
        }
    }
    public static  class WindowResult extends ProcessWindowFunction<Long, String,Integer, TimeWindow>{

        @Override
        public void process(Integer integer, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            out.collect("窗口"+new Timestamp(context.window().getStart())+"~"+new Timestamp(context.window().getEnd())+
                    "中的pv是:"+elements.iterator().next());
        }
    }
    public static class AggCount implements AggregateFunction<ClickEvent,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ClickEvent value, Long accumulator) {
            return accumulator+1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }
}

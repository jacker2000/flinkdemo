package com.gf.day04;

import com.gf.utils.ClickEvent;
import com.gf.utils.ClickSource;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Time;
import java.sql.Timestamp;

public class FlinkCustom_TumbleWindow_MapState_Long {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .keyBy(r->r.url)
                .process(new FakeTumbleWindow(10*1000L))
                .print();

        env.execute();
    }
    public static class FakeTumbleWindow extends KeyedProcessFunction<String, ClickEvent, String> {
        private Long windowSize;
        public FakeTumbleWindow (long windowSize){
            this.windowSize= windowSize;
        }
        //key:窗口开始时间
        //value: 累加器
        private MapState<Long,Long> windowMapStata;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            windowMapStata=getRuntimeContext().getMapState(
                    new MapStateDescriptor<Long, Long>(
                            "windowStart-List",
                            Types.LONG,
                            Types.LONG
                    )
            );
        }

        @Override
        public void processElement(ClickEvent value, Context ctx, Collector<String> out) throws Exception {
            //事件时间
            long currTs = ctx.timerService().currentProcessingTime();
            //根据窗口大小和事件事件获取窗口开始时间
            long windowStartTime = currTs - currTs % windowSize;

            if (!windowMapStata.contains(windowStartTime)) {
                //窗口第一条数据到达
                windowMapStata.put(windowStartTime,1L);
            }else {
                windowMapStata.put(windowStartTime, windowMapStata.get(windowStartTime)+1L);
            }
            //注册onTimer时间
            ctx.timerService().registerProcessingTimeTimer(windowStartTime+windowSize-1L);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            //获取开始时间
            long startTime = timestamp + 1 - windowSize;
            out.collect("url"+ctx.getCurrentKey()+"在窗口"+
                    new Timestamp(startTime)+"~"+
                    new Timestamp(startTime+windowSize)+"中的浏览数"+
                    windowMapStata.get(startTime));
            windowMapStata.remove(startTime);
        }
    }
}

package com.gf.test.day04;

import com.gf.utils.ClickEvent;
import com.gf.utils.ClickSource;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .keyBy(r->r.url)
                //设置滚动窗口大小
                .process(new FakeTumbleWindow(10*1000L))
                .print();

        env.execute();
    }
    public static class FakeTumbleWindow extends KeyedProcessFunction<String, ClickEvent,String>{
        private long windowSize;
        public FakeTumbleWindow(long windowSize){
            this.windowSize=windowSize;
        }
        //key:窗口开始时间
        //value: List<ClickEvent>
        //定义值状态
        private MapState<Long, List<ClickEvent>> windowMapStata;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            windowMapStata= getRuntimeContext().getMapState(
                    new MapStateDescriptor<Long, List<ClickEvent>>(
                            "map-stata",
                            Types.LONG,
                            Types.LIST(Types.POJO(ClickEvent.class))
                    )
            );
        }

        @Override
        public void processElement(ClickEvent value, Context ctx, Collector<String> out) throws Exception {
            //获取当前事件时间
            long currTs = ctx.timerService().currentProcessingTime();
            //窗口开始时间
            long windowStartTime = currTs - currTs % windowSize;
            //将元素添加到所属窗口,map累加器是否包含开始时间
            if (!windowMapStata.contains(windowStartTime)) {
                //窗口第一条数据到达
                List<ClickEvent> clickEvents = new ArrayList<>();
                 clickEvents.add(value);
                windowMapStata.put(windowStartTime,clickEvents);
            }else {
                windowMapStata.get(windowStartTime).add(value);
            }
            //在窗口结束时间-1毫秒注册定时器
            ctx.timerService().registerProcessingTimeTimer(windowStartTime+windowSize-1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);

            long windowStartTime = timestamp + 1 - windowSize;
            out.collect("url:"+ctx.getCurrentKey()+"在窗口"+
                    new Timestamp(windowStartTime)+"~"+
                    new Timestamp(windowStartTime+windowSize)+"中的浏览次数"+
                    windowMapStata.get(windowStartTime).size());

            //销毁窗口，这里是不能销毁整个定时器windowMapStata，因为它维护了多个不同开始时间的窗口
            windowMapStata.remove(windowStartTime);
        }
    }
}

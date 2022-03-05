package com.gf.day07;

import com.gf.utils.ClickEvent;
import com.gf.utils.ClickSource;
import com.gf.utils.UrlViewCountPerWindow;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class FlinkCustom_SlidingWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .keyBy(r->r.url)
                .process(new KeyedProcessFunction<String, ClickEvent, UrlViewCountPerWindow>() {
                    private long SlidingWindowSize =10 *1000L;
                    private long SlidingWIndowStop =5*1000L;
                    private MapState<Long, List<ClickEvent>> mapStata;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        mapStata =getRuntimeContext().getMapState(
                                new MapStateDescriptor<Long, List<ClickEvent>>(
                                        "map-stata",
                                        Types.LONG,
                                        Types.LIST(Types.POJO(ClickEvent.class))
                                )
                        );
                    }

                    @Override
                    public void processElement(ClickEvent value, Context ctx, Collector<UrlViewCountPerWindow> out) throws Exception {
                        //7秒事件
                        long currTime = ctx.timerService().currentProcessingTime();
                         List<Long> arr = new ArrayList<>();
                        long windowStartTime = getWindowStart(currTime, SlidingWIndowStop);
                        for (long start = windowStartTime; start > currTime - SlidingWindowSize; start -= SlidingWIndowStop) {
                            arr.add(start);
                        }
                        //对于 7秒到达的事件，arr[5,0]
                        for (Long startTime : arr) {
                            if (!mapStata.contains(startTime)) {
                                 List<ClickEvent> events = new ArrayList<>();
                                 events.add(value);
                                 mapStata.put(startTime,events);
                            }else {
                                mapStata.get(startTime).add(value);
                            }
                            //注册定时器
                            ctx.timerService().registerProcessingTimeTimer(startTime+SlidingWindowSize-1L);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<UrlViewCountPerWindow> out) throws Exception {
                        //触发窗口计算并销毁窗口
                        //计算出窗口结束时间
                        long windowEndTime = timestamp + 1L;
                        long windowStartTimes = windowEndTime - SlidingWindowSize;
                        String url = ctx.getCurrentKey();
                        //获取窗口中所有数据总数
                        Long count = Long.valueOf(mapStata.get(windowStartTimes).size());
                        //向下游发送数据
                        out.collect(new UrlViewCountPerWindow(
                                url,
                                count,
                                windowStartTimes,
                                windowEndTime
                        ));
                        mapStata.remove(windowStartTimes);
                    }
                }).print();
        env.execute();
    }
    public static long getWindowStart(long timestamp, long slide) {
        return timestamp - (timestamp  + slide) % slide;
    }
}

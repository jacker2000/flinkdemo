package com.gf.day05;

import com.gf.utils.ClickEvent;
import com.gf.utils.ClickSource;
import com.gf.utils.UrlViewCountPerWindow;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 *  每个url在每隔5秒滚动窗口中被浏览次数
 *  使用KeyedProcessFunction实现全窗口聚合函数的功能
 */
public class FlinkMapState_Pojo_KeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .keyBy(r->r.url)
                .process(new KeyedProcessFunction<String, ClickEvent, UrlViewCountPerWindow>() {
                    //key:windowStartTime
                    //value:url在窗口中浏览的次数，也就是累加器
                    private MapState<Long, List<ClickEvent>> mapState;

                    //滚动窗口长度
                    private long windowSize =5000L;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        mapState= getRuntimeContext().getMapState(
                                new MapStateDescriptor<Long, List<ClickEvent>>(
                                        "map-state",
                                        Types.LONG,
                                        Types.LIST(Types.POJO(ClickEvent.class))
                                )
                        );
                    }

                    @Override
                    public void processElement(ClickEvent value, Context ctx, Collector<UrlViewCountPerWindow> out) throws Exception {
                        //根据数据到达机器时间计算数据所属的窗口
                        long currTs = ctx.timerService().currentProcessingTime();
                        long windowStartTime = currTs - currTs % windowSize;
                        long windowEndTime = windowStartTime + windowSize;

                        if (!mapState.contains(windowStartTime)) {
                            //窗口的第一天数据到达，创建一个窗口对应新的列表
                             List<ClickEvent> enents = new ArrayList<>();
                             enents.add(value);
                             mapState.put(windowStartTime,enents);
                        }else {
                            //每来一条数据就把数据添加到列表中
                             mapState.get(windowStartTime).add(value);
                        }

                        ctx.timerService().registerProcessingTimeTimer(windowEndTime-1);
                    }

                    //onTimer 相当于processWindow里面的process函数
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<UrlViewCountPerWindow> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        //触发窗口计算并销毁窗口
                        //计算出窗口结束时间
                        long windowEndTime = timestamp + 1L;
                        long windowStartTimes = windowEndTime - windowSize;
                        String url = ctx.getCurrentKey();
                        //获取窗口中所有数据总数
                        Long count = Long.valueOf(mapState.get(windowStartTimes).size());
                        //向下游发送数据
                        out.collect(new UrlViewCountPerWindow(
                                url,
                                count,
                                windowStartTimes,
                                windowEndTime
                        ));
                        mapState.remove(windowStartTimes);
                    }
                })
                .print();

        env.execute();
    }
}

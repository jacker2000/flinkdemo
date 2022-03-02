package com.gf.day04;

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

/**
 *  每个url在每隔5秒滚动窗口中被浏览次数
 *  使用KeyedProcessFunction实现增量聚合函数和全窗口聚合函数结合使用
 */
public class FlinkMapState_ProcessWindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .keyBy(r->r.url)
                .process(new KeyedProcessFunction<String, ClickEvent, UrlViewCountPerWindow>() {

                    //key：windowStartTime
                    //value:url在windowStartTime对应窗口中浏览次数,也就是累加器
                    private MapState<Long,Long> mapState;

                    //滚动窗口长度
                    private long windowSize =5000L;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        mapState= getRuntimeContext().getMapState(
                                new MapStateDescriptor<Long, Long>(
                                        "start-acc",
                                        Types.LONG,
                                        Types.LONG
                                )
                        );
                    }

                    @Override
                    public void processElement(ClickEvent value, Context ctx, Collector<UrlViewCountPerWindow> out) throws Exception {
                        //根据数据到达集群时间计算数据所属窗口
                        long currTs= ctx.timerService().currentProcessingTime();
                        long windowStartTime= currTs-currTs%windowSize;
                        long windowEndTime= windowStartTime+windowSize;
                        //更新窗口中的累加器
                        if (!mapState.contains(windowStartTime)) {
                            //如果不包含windowStartTime这个Key,说明windowStartTime对应窗口中第一个元素到达
                            mapState.put(windowStartTime,1L);
                        }else {
                            //如果窗口已经存在，那么将窗口中的累加器+1
                            mapState.put(windowStartTime,mapState.get(windowStartTime)+1L);
                        }
                        //在窗口结束时间-1毫秒注册一个定时事件
                        ctx.timerService().registerProcessingTimeTimer(windowEndTime-1L);

                    }
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<UrlViewCountPerWindow> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        //用来触发窗口的计算并销毁
                        //计算出窗口结束时间
                        long windowEndTime= timestamp+1L;
                        long windowStartTime = windowEndTime-windowSize;
                        String url = ctx.getCurrentKey();
                        //获取窗口中累加器的值
                        long count =mapState.get(windowStartTime);
                        //向下游发送数据
                        out.collect(
                                new UrlViewCountPerWindow(
                                        url,
                                        count,
                                        windowStartTime,
                                        windowEndTime
                                )
                        );
                        //销毁窗口
                        //.remove只清除windowStartTime对应的键值对
                        //.clear 将整个mapState清空
                        mapState.remove(windowStartTime);
                    }
                })
                .print();
        env.execute();
    }
}

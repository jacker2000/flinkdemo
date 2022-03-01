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

/**
 *  每个url在每隔5秒滚动窗口中被浏览次数
 *  使用KeyedProcessFunction实现全窗口聚合函数的功能
 */
public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .keyBy(r->r.url)
                .process(new KeyedProcessFunction<String, ClickEvent, UrlViewCountPerWindow>() {
                    //key:windowStartTime
                    //value:url在窗口中浏览的次数，也就是累加器
                    private MapState<Long,Long> mapState;

                    //滚动窗口长度
                    private long windowSize =5000L;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        mapState= getRuntimeContext().getMapState(
                                new MapStateDescriptor<Long, Long>(
                                        "map-state",
                                        Types.LONG,
                                        Types.LONG

                                )
                        );
                    }

                    @Override
                    public void processElement(ClickEvent value, Context ctx, Collector<UrlViewCountPerWindow> out) throws Exception {

                    }
                })
                .print();

        env.execute();
    }
}

package com.gf.day07;

import com.gf.utils.ItemViewCountPerWindow;
import com.gf.utils.UrlViewCountPerWindow;
import com.gf.utils.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

public class FlinkKafkaConsumer_Flie {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.setProperty("group.id", "consumer-group");

        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        env
                .addSource(new org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer<String>(
                        "userbehavior",//topic名称
                        new SimpleStringSchema(),
                        properties
                ))
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new UserBehavior(
                                arr[0],
                                arr[1],
                                arr[2],
                                arr[3],
                                Long.parseLong(arr[4])*1000L
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.ts;
                            }
                        })
                )
                .keyBy(r->r.itemId)
                .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(5)))
                .aggregate(new AggCount(),new WindowResult())
                .keyBy(r->r.windowEndTime)
                .process(new KeyedProcessFunction<Long, ItemViewCountPerWindow, String>() {
                    private ListState<ItemViewCountPerWindow> listState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        listState= getRuntimeContext().getListState(
                                new ListStateDescriptor<ItemViewCountPerWindow>(
                                        "list-",
                                        Types.POJO(ItemViewCountPerWindow.class)
                                )
                        );
                    }

                    @Override
                    public void processElement(ItemViewCountPerWindow value, Context ctx, Collector<String> out) throws Exception {
                        listState.add(value);
                        //降序排序
                         List<ItemViewCountPerWindow> list = new ArrayList<>();
                        for (ItemViewCountPerWindow e : listState.get()) {
                            list.add(e);
                        }
                        //这里不能把listState清空，因为是实时的，不是定时
                        list.sort(new Comparator<ItemViewCountPerWindow>() {
                            @Override
                            public int compare(ItemViewCountPerWindow o1, ItemViewCountPerWindow o2) {
                                return o2.count.intValue()-o1.count.intValue();
                            }
                        });
                        if (list.size()>2) {
                            StringBuilder builder = new StringBuilder();
                            builder.append("=======\n");
                            builder.append("窗口"+new Timestamp(value.windowStartTime)+"~"+new Timestamp(value.windowEndTime)
                            +"\n");
                            for (int i = 0; i < 3; i++) {
                                ItemViewCountPerWindow tmp = list.get(i);
                                builder.append("第"+(i+1)+"名商品ID是:"+tmp.itemId+"，浏览次数是:"+tmp.count+"\n");
                            }
                            builder.append("=====================\n");
                            out.collect(builder.toString());
                        }
                    }
                }).print();
        env.execute();
    }
    public static class WindowResult extends ProcessWindowFunction<Long, ItemViewCountPerWindow,String, TimeWindow>{

        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<ItemViewCountPerWindow> out) throws Exception {
            out.collect(
                    new ItemViewCountPerWindow(
                            s,
                            elements.iterator().next(),
                            context.window().getStart(),
                            context.window().getEnd()
                    )
            );
        }
    }
    public static class  AggCount implements AggregateFunction<UserBehavior,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
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

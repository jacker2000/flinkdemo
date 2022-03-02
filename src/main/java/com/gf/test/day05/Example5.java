package com.gf.test.day05;

import com.gf.utils.ItemViewCountPerWindow;
import com.gf.utils.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

public class Example5 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .readTextFile("D:\\IdeaProjects\\bigdataCode\\flink_0906\\src\\main\\resources\\UserBehavior.csv")
                //对文本数据分隔处理成UserBehavior
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        //读取每行数据，通过,分隔
                        String[] arr = value.split(",");
                        return new UserBehavior(
                                arr[0], arr[1], arr[2], arr[3],
                                Long.parseLong(arr[4]) * 1000L
                        );
                    }
                })
                //把数据过滤为类型为pv的数据
                .filter(r -> r.type.equals("pv"))
                //设置水位线时间戳，超时时间为0，且UserBehavior中以ts为时间戳
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        return element.ts;
                                    }
                                })
                )
                //key分组(UserBehavior)
                .keyBy(r -> r.itemId)
                //设置为滑动窗口1小时，每次步长5分钟
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new CoutAgg(), new WindowResult())
                .keyBy(r -> r.windowEndTime)//ItemViewCountPerWindow
                .process(new TopN(3))
                .print();


        env.execute();
    }
    //输入，统计，输出
    public static class CoutAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
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
    }
    //输入，输出，key，window
    public static class WindowResult extends ProcessWindowFunction<Long, ItemViewCountPerWindow, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<ItemViewCountPerWindow> out) throws Exception {

            out.collect(new ItemViewCountPerWindow(
                    s,
                    elements.iterator().next(),
                    context.window().getStart(),
                    context.window().getEnd()
            ));
        }
    }

    public static class TopN extends KeyedProcessFunction<Long, ItemViewCountPerWindow, String> {
        private int n;

        public TopN(int n) {
            this.n = n;
        }
        private ListState<ItemViewCountPerWindow> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState= getRuntimeContext().getListState(
                    new ListStateDescriptor<ItemViewCountPerWindow>(
                            "list-state",
                            Types.POJO(ItemViewCountPerWindow.class)
                    )
            );
        }

        @Override
        public void processElement(ItemViewCountPerWindow value, Context ctx, Collector<String> out) throws Exception {
            listState.add(value);

            //注册事件定时器(保证windowEndTime对应窗口所有ItemViewCountPerWindow都到齐)
            ctx.timerService().registerEventTimeTimer(value.windowEndTime+1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<ItemViewCountPerWindow> arrayList = new ArrayList<>();
            for (ItemViewCountPerWindow e : listState.get()) {
                arrayList.add(e);
            }
            listState.clear();
            arrayList.sort(new Comparator<ItemViewCountPerWindow>() {
                @Override
                public int compare(ItemViewCountPerWindow o1, ItemViewCountPerWindow o2) {
                    //必须降序排列
                    return (int)(o2.count-o1.count);
                }
            });
            //格式化输出
            StringBuilder result = new StringBuilder();
            result.append("======================\n");
            result.append("窗口结束时间"+new Timestamp(timestamp-1000L)+"\n ");
            for (int i = 0; i < n; i++) {
                ItemViewCountPerWindow itemViewCountPerWindow = arrayList.get(i);
                result.append("第"+(i+1)+"商品ID是"+itemViewCountPerWindow.itemId+"浏览次数是:"+itemViewCountPerWindow.count+"\n");
            }
            result.append("=============================\n");
            out.collect(result.toString());

        }
    }




}

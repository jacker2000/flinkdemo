package com.gf.day06;

import com.gf.utils.ItemViewCountPerWindow;
import com.gf.utils.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * 实时热门商品
 * 每隔5分钟计算一次过去一小时的pv最多的3个商品
 */
public class FlinkTopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<UserBehavior> stream = env
                .readTextFile("D:\\IdeaProjects\\bigdataCode\\flink_0906\\src\\main\\resources\\UserBehavior.csv")
                //输入，输出
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new UserBehavior(
                                arr[0],
                                arr[1],
                                arr[2],
                                arr[3],
                                Long.parseLong(arr[4])
                        );
                    }
                })
                .filter(r -> r.type.equals("pv"))
                //设置水位线
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        return element.ts;
                                    }
                                })
                );
        stream
                .keyBy(r -> r.itemId)
                //滑动窗口大小1小时，步长5分钟
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))

                .aggregate(new CountAgg(), new WindowResult())
                .keyBy(r -> r.windowEndTime)
                .process(new TopN(3))
                .print();

        env.execute();
    }

    //key,in,out
    public static class TopN extends KeyedProcessFunction<Long, ItemViewCountPerWindow, String> {

        private int n;

        public TopN(int n) {
            this.n = n;
        }

        //对来的数据进行排序,数据类型为List.sort
        private ListState<ItemViewCountPerWindow> listState;

        //定义要注册的初始事件时间
        private ValueState<Long> timeTs ;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ItemViewCountPerWindow>(
                            "list-state",
                            Types.POJO(ItemViewCountPerWindow.class)
                    )
            );
            timeTs= getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("time_end",Types.LONG)
            );
        }

        @Override
        public void processElement(ItemViewCountPerWindow value, Context ctx, Collector<String> out) throws Exception {
            //将数据添加到listState 列表状态变量中，并对其排序
            listState.add(value);


            /**
             * todo
             *   注册事件时间
             *   当大于等于value.windowEndTime +100L的水位线到达KeyedProcessFunction以后
             *   value.windowEndTime 所标识的窗口中的所有统计信息都已到达
             */
            ctx.timerService().registerEventTimeTimer(value.windowEndTime+1000L);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            super.onTimer(timestamp, ctx, out);
            //将数据从listState中取出，并放入ArrayList
            ArrayList<ItemViewCountPerWindow> list = new ArrayList<>();
            for (ItemViewCountPerWindow itemViewCountPerWindow : listState.get()) {
                list.add(itemViewCountPerWindow);
            }
            //将listState清空，节省内存
            listState.clear();
            list.sort(new Comparator<ItemViewCountPerWindow>() {
                @Override
                public int compare(ItemViewCountPerWindow o1, ItemViewCountPerWindow o2) {
                    //记住是降序排序
                    return (int)(o2.count-o1.count);
                }
            });
            //格式化输出
            StringBuilder result = new StringBuilder();
            result.append("======================\n");
            result.append("窗口结束时间"+new Timestamp(timestamp-1000L)+"\n ");
            for (int i = 0; i < n; i++) {
                ItemViewCountPerWindow itemViewCountPerWindow = list.get(i);
                result.append("第"+(i+1)+"商品ID是"+itemViewCountPerWindow.itemId+"浏览次数是:"+itemViewCountPerWindow.count+"\n");
            }
            result.append("=============================\n");
            out.collect(result.toString());

        }
    }

    //输入，统计，输出(目的是为了统计，肯定是Long)
    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

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

    //输入(聚合的输出值)，输出(用户信息),key,window
    public static class WindowResult extends ProcessWindowFunction<Long, ItemViewCountPerWindow, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<ItemViewCountPerWindow> out) throws Exception {
            out.collect(new ItemViewCountPerWindow(
                    key,
                    elements.iterator().next(),
                    context.window().getStart(),
                    context.window().getEnd()
            ));
        }
    }

}

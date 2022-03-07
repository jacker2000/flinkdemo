package com.gf.day08;

import com.gf.day05.FlinkReadFile_TopN;
import com.gf.utils.ItemViewCountPerWindow;
import com.gf.utils.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 *  每小时UV统计
 *  uv是pv按照userID进行去重后的结果
 */
public class FlinkDistinctStatisticsUV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<UserBehavior> stream = env
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
                );

        stream.keyBy(r->1)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new CoutAgg(), new WindowResult())
                .print();

        env.execute();
    }
    //输入，统计，输出
    public static class CoutAgg implements AggregateFunction<UserBehavior, HashSet<String>, Long> {

        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public HashSet<String> add(UserBehavior value, HashSet<String> accumulator) {
            accumulator.add(value.userId);
            return accumulator;
        }

        @Override
        public Long getResult(HashSet<String> accumulator) {
            return (long)accumulator.size();
        }

        @Override
        public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
            return null;
        }
    }
    //输入，输出，key，window
    public static class WindowResult extends ProcessWindowFunction<Long, String, Integer, TimeWindow> {
        @Override
        public void process(Integer integer, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            out.collect("窗口"+new Timestamp(context.window().getStart())+"~"
                    +new Timestamp(context.window().getEnd())+"中的UV是:"+elements.iterator().next());
        }

    }
}

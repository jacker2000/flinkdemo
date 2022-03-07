package com.gf.day09;

import com.gf.utils.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.com.google.common.base.Charsets;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class Example1 {
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

        stream.keyBy(r -> 1)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new CoutAgg(), new WindowResult())
                .print();

        env.execute();
    }

    //输入，输出，key，window
    public static class WindowResult extends ProcessWindowFunction<Long, String, Integer, TimeWindow> {
        @Override
        public void process(Integer integer, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            out.collect("窗口" + new Timestamp(context.window().getStart()) + "~"
                    + new Timestamp(context.window().getEnd()) + "中的UV是:" + elements.iterator().next());
        }
    }
    //输入，统计，输出
    public static class CoutAgg implements AggregateFunction<UserBehavior, Tuple2<BloomFilter<String>,Long>,Long> {


        @Override
        public Tuple2<BloomFilter<String>, Long> createAccumulator() {
            return  Tuple2.of(
                    //参数(去重的数据类型，预期要去重的数量，误判率)
                    BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8),10000,0.01),
                    //确定没来过的用户数量
                    0L
            );
        }

        @Override
        public Tuple2<BloomFilter<String>, Long> add(UserBehavior value, Tuple2<BloomFilter<String>, Long> accumulator) {
            if (!accumulator.f0.mightContain(value.userId)) {
                //将相关bit置为1
                accumulator.f0.put(value.userId);
                //用户一定没来过,统计值加1
                accumulator.f1+=1;
            }
            return accumulator;
        }

        @Override
        public Long getResult(Tuple2<BloomFilter<String>, Long> accumulator) {
            return accumulator.f1;
        }

        @Override
        public Tuple2<BloomFilter<String>, Long> merge(Tuple2<BloomFilter<String>, Long> a, Tuple2<BloomFilter<String>, Long> b) {
            return null;
        }

    }

}

package com.gf.day08;

import com.gf.utils.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.com.google.common.base.Charsets;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 *  BloomFilter:
 *         用容错率 换空间
 *         在大容量的数据前提下，允许个别没有命中
 */
public class FlinkBloomFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.readTextFile("D:\\IdeaProjects\\bigdataCode\\flink_0906\\src\\main\\resources\\UserBehavior.csv")
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
                .filter(r->r.type.equals("pv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.ts;
                            }
                        })
                )
                .keyBy(r->1)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new AggregateFunction<UserBehavior, Tuple2<BloomFilter<String>, Long>, Long>() {


                               @Override
                               public Tuple2<BloomFilter<String>, Long> createAccumulator() {
                                   return Tuple2.of(
                                           BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8),//待去重数据类型
                                                   50000,//待去重数据量
                                                   0.01), //误判率
                                           0L //统计值
                                   );
                               }

                               @Override
                               public Tuple2<BloomFilter<String>, Long> add(UserBehavior value, Tuple2<BloomFilter<String>, Long> accumulator) {
                                   if (!accumulator.f0.mightContain(value.userId)) {
                                       accumulator.f0.put(value.userId);
                                       accumulator.f1 += 1L;
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
                        , new ProcessWindowFunction<Long, String, Integer, TimeWindow>() {
                            @Override
                            public void process(Integer integer, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
                                out.collect("窗口:"+new Timestamp(context.window().getStart())+"~"
                                +new Timestamp(context.window().getEnd())+"里的独立访客数是:"+elements.iterator().next());
                            }
                        })
                .print();

        env.execute();
    }


}

package com.gf.day05;

import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//水位线测试
public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env
//                .socketTextStream("localhost",9999)
//                .map(new MapFunction<String, Tuple2<String,Long>>() {
//                    @Override
//                    public Tuple2<String, Long> map(String value) throws Exception {
//                        String[] arr = value.split(" ");
//
//                        return Tuple2.of(arr[0],
//                                Long.parseLong(arr[1])*1000L); //
//                    }
//                })
                //在map输出的数据流中插入水位线
                //默认每隔200毫秒的机器时间插入一次水位线
                //每次插入水位线时，插入多大的水位线呢
                // 水位线= 观察到最大时间戳-最大延迟时间-1毫秒
//                .assignTimestampsAndWatermarks(
//                        //设置最大延迟时间是5秒钟
////                        WatermarkStrategy<Tuple2<String,Long>>
//                )
//                .print();
        env.execute();
    }
}

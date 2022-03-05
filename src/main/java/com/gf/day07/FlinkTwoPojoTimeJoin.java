package com.gf.day07;

import com.gf.utils.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class FlinkTwoPojoTimeJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<OrderEvent> orderStream = env
                .fromElements(
                        new OrderEvent("order-1", "pay", 12 * 60 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                                    @Override
                                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                                        return element.ts;
                                    }
                                })
                );
        SingleOutputStreamOperator<OrderEvent> shipStream = env
                .fromElements(
                        new OrderEvent("order-1", "ship", 10 * 60 * 60 * 1000L),
                        new OrderEvent("order-1", "ship", 13 * 60 * 60 * 1000L),
                        new OrderEvent("order-1", "ship", 14 * 60 * 60 * 1000L),
                        new OrderEvent("order-1", "ship", 24 * 60 * 60 * 1000L),
                        new OrderEvent("order-1", "ship", 48 * 60 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                                    @Override
                                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                                        return element.ts;
                                    }
                                })
                );
        orderStream.keyBy(r->r.orderId)
                .intervalJoin(shipStream.keyBy(r->r.orderId))
                //过期1天~未来1天
                .between(Time.days(-1),Time.days(1))
                .process(new ProcessJoinFunction<OrderEvent, OrderEvent, String>() {
                    @Override
                    public void processElement(OrderEvent left, OrderEvent right, Context ctx, Collector<String> out) throws Exception {
                        out.collect(left+"=>"+right);
                    }
                })
                .print();
        env.execute();
    }
}

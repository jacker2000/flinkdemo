package com.gf.day07;

import com.gf.utils.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * 根据间隔的join
 */
public class FlinkTwoPojoTimeJoin1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<OrderEvent> buyStream = env
                .fromElements(
                        new OrderEvent("order-1", "buy", 48 * 60 * 60 * 1000L)
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
                        new OrderEvent("order-1", "ship", 32 * 60 * 60 * 1000L),
                        new OrderEvent("order-1", "ship", 60 * 60 * 60 * 1000L),
                        new OrderEvent("order-1", "ship", 72 * 60 * 60 * 1000L),
                        new OrderEvent("order-1", "ship", 84 * 60 * 60 * 1000L),
                        new OrderEvent("order-1", "ship", 95* 60 * 60 * 1000L),
                        new OrderEvent("order-1", "ship", 96 * 60 * 60 * 1000L),

                        new OrderEvent("order-1", "ship", 200 * 60 * 60 * 1000L)
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
        buyStream.keyBy(r->r.orderId)
                .intervalJoin(shipStream.keyBy(r->r.orderId))
                .between(Time.days(-1),Time.days(3))
                .process(new ProcessJoinFunction<OrderEvent, OrderEvent, String>() {
                    @Override
                    public void processElement(OrderEvent left, OrderEvent right, Context ctx, Collector<String> out) throws Exception {
                        out.collect(left.orderId+":"+new Timestamp(left.ts)+"=>"+new Timestamp(right.ts));
                    }
                }).print();
        env.execute();
    }
}

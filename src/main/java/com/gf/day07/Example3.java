package com.gf.day07;

import com.gf.utils.OrderEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

//实时对账
// app的支付信息和第三方支付信息的对账
public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<OrderEvent> appZhifuStream = env
                .addSource(new SourceFunction<OrderEvent>() {
                    @Override
                    public void run(SourceContext<OrderEvent> ctx) throws Exception {
                        OrderEvent e1 = new OrderEvent("order-1", "app-zhifu", 1000L);
                        ctx.collectWithTimestamp(e1, e1.ts);
                        Thread.sleep(1000L);
                        OrderEvent e2 = new OrderEvent("order-2", "app-zhifu", 2000L);
                        ctx.collectWithTimestamp(e2, e2.ts);
                        Thread.sleep(1000L);
                        ctx.emitWatermark(new Watermark(7000L));
                        Thread.sleep(1000L);
                    }
                    @Override
                    public void cancel() {

                    }
                });
        DataStreamSource<OrderEvent> weixinStream = env
                .addSource(new SourceFunction<OrderEvent>() {
                    @Override
                    public void run(SourceContext<OrderEvent> ctx) throws Exception {
                        OrderEvent e1 = new OrderEvent("order-1", "weixin-zhifu", 4000L);
                        ctx.collectWithTimestamp(e1, e1.ts);
                        Thread.sleep(1000L);

                        ctx.emitWatermark(new Watermark(7000L));
                        Thread.sleep(1000L);

                        OrderEvent e2 = new OrderEvent("order-2", "weixin-zhifu", 9000L);
                        ctx.collectWithTimestamp(e2, e2.ts);
                        Thread.sleep(1000L);
                    }

                    @Override
                    public void cancel() {

                    }
                });

        appZhifuStream.keyBy(r->r.orderId)
                .connect(weixinStream.keyBy(r->r.orderId))
                .process(new Match())
                .print();
        env.execute();
    }
    public static class Match extends CoProcessFunction<OrderEvent,OrderEvent,String> {
        private ValueState<OrderEvent> appZhifuState;
        private ValueState<OrderEvent> weixinZhifuState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            appZhifuState= getRuntimeContext().getState(
                    new ValueStateDescriptor<OrderEvent>("appzhifu", Types.POJO(OrderEvent.class))
            );
            weixinZhifuState= getRuntimeContext().getState(
                    new ValueStateDescriptor<OrderEvent>("weixinzhifu", Types.POJO(OrderEvent.class))
            );
        }

        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
            if (weixinZhifuState.value()==null) {
                //说明 app-zhifu先到达，将app-zhifu保存下来，并等待对应的weixin支付5秒钟
                appZhifuState.update(value);
                ctx.timerService().registerEventTimeTimer(value.ts+5000L);
            }else {
                //weixinzhifuState中已经保存了weixin支付，说明weixin支付先到
                out.collect("订单"+value.orderId+"对账成功,weixin支付先到达,app支付后到达");
                //将weixinzhifuState清空
                weixinZhifuState.clear();
            }
        }

        @Override
        public void processElement2(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
            //与processElement1完全对称
            if (appZhifuState.value()==null) {
                weixinZhifuState.update(value);
                ctx.timerService().registerEventTimeTimer(value.ts+5000L);
            }else {
                out.collect("订单"+value.orderId+"对账成功,app支付先到达,weixin支付后到达");
                appZhifuState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            if (appZhifuState.value()!=null) {
                out.collect("订单"+appZhifuState.value().orderId+"对账失败,weixin支付未到达");
                appZhifuState.clear();
            }
            if (weixinZhifuState.value()!=null) {
                out.collect("订单"+weixinZhifuState.value().orderId+"对账失败,app支付未到达");
                weixinZhifuState.clear();
            }
        }
    }
}

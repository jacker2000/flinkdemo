package com.gf.day06;

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

// 实时对账
// app的支付流
// weixin支付流
// app支付和weixin支付，谁先到，就把谁保存下来，等待对方
public class FlinkRealtimeAccount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<OrderEvent> appStream = env
                .addSource(new SourceFunction<OrderEvent>() {
                    //不是原子性,会有另外一个流同时执行另一个run,两个run交叉执行
                    @Override
                    public void run(SourceContext<OrderEvent> ctx) throws Exception {
                        ctx.collectWithTimestamp(new OrderEvent("order-1", "app-zhifu", 1000L), 1000L);
                        Thread.sleep(1000L);
                        ctx.collectWithTimestamp(new OrderEvent("order-2", "app-zhifu", 4000L), 4000L);
                        Thread.sleep(1000L);
                        ctx.emitWatermark(new Watermark(9000L));
                        Thread.sleep(1000L);
                    }

                    @Override
                    public void cancel() {

                    }
                });
        DataStreamSource<OrderEvent> weixinStream = env.addSource(new SourceFunction<OrderEvent>() {
            //不是原子性,会有另外一个流同时执行另一个run,两个run交叉执行
            @Override
            public void run(SourceContext<OrderEvent> ctx) throws Exception {
                ctx.collectWithTimestamp(new OrderEvent("order-1", "weixin-zhifu", 3000L), 3000L);
                Thread.sleep(1000L);
                ctx.emitWatermark(new Watermark(9000L));
                Thread.sleep(1000L);
                ctx.collectWithTimestamp(new OrderEvent("order-2", "weixin-zhifu", 11000L), 11000L);
                Thread.sleep(1000L);
            }

            @Override
            public void cancel() {

            }
        });

        appStream.keyBy(r->r.orderId)
                .connect(weixinStream.keyBy(r->r.orderId))
                .process(new Match())
                .print();

        env.execute();
    }
    public static class Match extends CoProcessFunction<OrderEvent,OrderEvent,String>{

        private ValueState<OrderEvent> appState;
        private ValueState<OrderEvent> weixinState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            appState=getRuntimeContext().getState(
                    new ValueStateDescriptor<OrderEvent>("app-state", Types.POJO(OrderEvent.class))
            );
            weixinState=getRuntimeContext().getState(
                    new ValueStateDescriptor<OrderEvent>("weixin-state", Types.POJO(OrderEvent.class))
            );
        }

        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
            //app数据先过来
            if (weixinState.value()==null) {
                //weixin数据为null,app支付数据先到,保存更新appState
                appState.update(value);
                //注册的定时器一定会被触发
                ctx.timerService().registerEventTimeTimer(value.ts+5000L);//定时器:app时间时间+5000
            }else {
                //app数据先到，在app定时器范围内，微信的数据不为null,则对账成功(两个数据都过来)
                out.collect(value.orderId+"对账成功,weixin支付先到");//在app定时范围内，winxin数据到达,则对账成功
                //app先到，对账成功后清除weixinState
                weixinState.clear();
            }
        }
        @Override
        public void processElement2(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
            if (appState.value()==null) {
                //app数据为null,保存weixin
                weixinState.update(value);
                ctx.timerService().registerEventTimeTimer(value.ts+5000L);
            }else {
                out.collect(value.orderId+"对账成功,app支付先到");//在weinxin定时范围内，app数据到达，则对账成功
                //weixin先到,app数据在定时器范围内到达，则对账成功,清除appState数据
                appState.clear();
            }
        }
        /**
         * todo
         *   结果1:
         *      order-1对账成功,weixin支付先到(会清除weixinState，onTimer之后触发后不会打印)
         *      order-2对账成功,app支付先到(会清除appState,onTimer之后触发后不会打印)
         *   分析;
         *      1.先走weinxin数据3000L数据，再app数据
         *          weixin注册8000L,16000定时器
         *          app注册6000L,9000L定时器
         *          a)weixin走完数据时,注册8000定时器，16000定时器，再执行app中的order1,order2,
         *              此时app执行order1时，weixin和app数据都在,对账成功,
         *              之后app在执行order2时,weixin数据也在，则对账成功
         *   结果2:
         *      order-1对账成功,app支付先到
         *      order-2对账失败,对应weixin支付没来(weixinState.value数据不为null,清空weixinState)
         *      order-2对账失败，对应app支付没来(appState.value()数据不为null,清空appState)
         *   分析：
         *      2.当app所有数据都走完，再走weixin数据
         *        app端会注册6000L,9000L定时器
         *        weixin会注册8000L,16000定时器
         *        a)app顺序走完之后，当weixin走第一条数据时,app和weixin的两条数据同时存在，对账成功，清空appState
         *          然后推高weixin推高水位线,触发app端定时器执行，
         *          此时order-1的appState数据为null,对账成功，order-2的appState不为null,order-2的app对账失败
         *          然后再次执行wein数据流，更新11000L时间戳,此时注册16000L定时器，当触发定时器的时候，order-2 appState不为null,weixin对账失败
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            if (appState.value()!=null) { //app定时器，定时器被触发，说明weixin数据没来
                out.collect(appState.value().orderId+"对账失败,对应weixin支付没来");
                //app定时的范围内,weixin数据支付没来，还是对账失败，并且清除appState数据
                appState.clear();
            }
            if (weixinState.value()!=null) { //weixin定时器，定时器被触发，说明app数据没来
                out.collect(weixinState.value().orderId+"对账失败，对应app支付没来");
                //app支付没来，则清除weixinState
                weixinState.clear();
            }
        }
    }
}

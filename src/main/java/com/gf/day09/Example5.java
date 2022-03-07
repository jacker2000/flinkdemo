package com.gf.day09;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

//超时未支付订单监测

public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple3<String, String, Long>> stream = env.addSource(new SourceFunction<Tuple3<String, String, Long>>() {
            @Override
            public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {
                ctx.collectWithTimestamp(Tuple3.of("order-1", "create", 1000L), 1000L);
                Thread.sleep(1000L);
                ctx.collectWithTimestamp(Tuple3.of("order-2", "create", 2000L), 2000L);
                Thread.sleep(1000L);
                ctx.collectWithTimestamp(Tuple3.of("order-1", "pay", 4000L), 4000L);
                Thread.sleep(1000L);
                ctx.emitWatermark(new Watermark(7000L));
                Thread.sleep(1000L);
                ctx.collectWithTimestamp(Tuple3.of("order-2", "pay", 9000L), 9000L);
                Thread.sleep(1000L);
            }

            @Override
            public void cancel() {

            }
        });
        stream.keyBy(r->r.f0)
                .process(new KeyedProcessFunction<String, Tuple3<String, String, Long>, String>() {
                    private ValueState<Tuple3<String,String,Long>> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        valueState =getRuntimeContext().getState(
                                new ValueStateDescriptor<Tuple3<String, String, Long>>(
                                        "event",
                                        Types.TUPLE(Types.STRING,Types.STRING,Types.LONG)
                                )
                        );
                    }

                    @Override
                    public void processElement(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        if (value.f1.equals("create")) {
                            valueState.update(value);
                            ctx.timerService().registerProcessingTimeTimer(value.f2+5000L);
                        }else if(valueState.value()!=null && valueState.value().f1.equals("create")){
                            out.collect("订单"+value.f0+"正常支付");
                            valueState.clear();
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        if (valueState.value()!=null && valueState.value().f1.equals("create")) {
                            out.collect("订单"+valueState.value().f0+"超时未支付");
                            valueState.clear();
                        }
                    }
                })
                .print();
        env.execute();
    }
}

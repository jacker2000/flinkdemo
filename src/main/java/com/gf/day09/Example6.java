package com.gf.day09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
//有限状态机连续3次登录失败监测
public class Example6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple3<String, String, Long>> loginStream = env.fromElements(
                Tuple3.of("user-1", "fail", 1000L),
                Tuple3.of("user-1", "fail", 2000L),
                Tuple3.of("user-2", "success", 3000L),
                Tuple3.of("user-1", "fail", 4000L),
                Tuple3.of("user-1", "fail", 5000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        })
        );
        loginStream
                .keyBy(r->r.f0)
                .process(new StateMachine())
                .print();
        env.execute();
    }
    public static class StateMachine extends KeyedProcessFunction<String,Tuple3<String,String,Long>,String>{
        private ValueState<String> currentState;
        private HashMap<Tuple2<String,String>,String> stateMachine=new HashMap<>();
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            currentState =getRuntimeContext().getState(
                    new ValueStateDescriptor<String>("currt-state", Types.STRING)
            );
            //状态机的保存
            //INITIAL状态机遇见fail事件，跳转到S1状态
            stateMachine.put(Tuple2.of("INITIAL","fail"),"S1");
            stateMachine.put(Tuple2.of("S1","fail"),"S2");
            stateMachine.put(Tuple2.of("S2","fail"),"FAIL");
            stateMachine.put(Tuple2.of("INITIAL","success"),"success");
            stateMachine.put(Tuple2.of("S1","success"),"success");
            stateMachine.put(Tuple2.of("S2","success"),"success");
        }

        @Override
        public void processElement(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            if (currentState.value()==null) {
                currentState.update("INITIAL");
            }
            //将要跳转到的状态
            String nextState = stateMachine.get(Tuple2.of(currentState.value(), value.f1));
            if (nextState.equals("FAIL")) {
                out.collect(value.f0+"连续三次登录失败");
                currentState.update("S2");
            }else if(nextState.equals("success")){
                currentState.update("INITIAL");
            }else {
                currentState.update(nextState);
            }
        }
    }
}

package com.gf.day03.richFunction;

import com.gf.utils.ClickEvent;
import com.gf.utils.ClickSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class FlinkValueState_Integer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                //用户名作为key
                .keyBy(r->r.username)
                .process(new KeyedProcessFunction<String, ClickEvent, String>() {
                    private ValueState<Integer> acc;
                    private ValueState<Long> timerTs;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        acc= getRuntimeContext().getState(
                                new ValueStateDescriptor<Integer>("acc", Types.INT)
                        );
                    }
                    @Override
                    public void processElement(ClickEvent value, Context ctx, Collector<String> out) throws Exception {
                        if (acc.value()==null) {
                            acc.update(1);
                        }else {
                            acc.update(acc.value()+1);
                        }
                        out.collect("用户:"+ctx.getCurrentKey()+"的pv数据是:"+acc.value());
                    }
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                    }
                })
                .print();


        env.execute();
    }
}

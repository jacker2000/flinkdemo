package com.gf.day04;

import com.gf.utils.SensorReading;
import com.gf.utils.SensorSource;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

//连续1秒钟温度上升的持续时间是多少
public class FlinkContinuousTemperatureRecord {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SensorSource())
                .keyBy(r->r.sensorId)
                .process(new KeyedProcessFunction<String, SensorReading, String>() {
                    private ValueState<Double> lastTemp; //温度
                    private ValueState<Long> timerTs; //定时器

//                    private ValueState<Long> durationTs; //持续时间
//
//                    private ListState<Double> timerList; //

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastTemp =getRuntimeContext().getState(
                                new ValueStateDescriptor<Double>("last_temp",
                                        Types.DOUBLE)
                        );
                        timerTs=getRuntimeContext().getState(
                                new ValueStateDescriptor<Long>("timer-ts",
                                        Types.LONG)
                        );

//                        durationTs= getRuntimeContext().getState(
//                                new ValueStateDescriptor<Long>("duration-Ts",
//                                        Types.LONG)
//                        );
//                        timerList =getRuntimeContext().getListState(
//                                new ListStateDescriptor<Double>("list-timerList",
//                                        Types.DOUBLE)
//                        );
                    }
                    @Override
                    public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
                        //取出最近一次累加器中温度数据
                        Double tmp = lastTemp.value();
                        //用累加器保存当前温度
                        lastTemp.update(value.temperature);

                        long startTime = ctx.timerService().currentProcessingTime();

//                        if (durationTs.value()==null){
//                            durationTs.update(startTime);
//                        }

                        //累加器中温度值不为null
                        if (tmp!=null) {
                            //1.温度上升2.不存在报警定时器
                            if (value.temperature>tmp&& timerTs.value()==null) {
                                long oneSecondLater = ctx.timerService().currentProcessingTime() + 1000L;
                                //注册定时器
                                ctx.timerService().registerProcessingTimeTimer(oneSecondLater);
                                timerTs.update(oneSecondLater);

//                                durationTs.update(ctx.timerService().currentProcessingTime()-durationTs.value());
//                                timerList.add(value.temperature);

                            }//1.温度下降2.存在报警定时器
                            else if(value.temperature<tmp&& timerTs.value()!=null){
                                ctx.timerService().deleteProcessingTimeTimer(timerTs.value());
                                timerTs.clear();
//                                durationTs.clear();
//                                timerList.clear();
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        StringBuilder str=new StringBuilder();
//                        Iterator<Double> temperatureList = timerList.get().iterator();
//                        while (temperatureList.hasNext()){
//                            str.append(temperatureList.next()+",");
//                        }
                        String resultStr = str.toString();
                        String result = resultStr.substring(0, resultStr.length() - 1);

                        out.collect("传感器" + ctx.getCurrentKey()
//                                +"连续1秒钟温度上升的持续时间:"+durationTs.value()+"毫秒"+
//                                "温度值列表为:"+result
                        );
                        timerTs.clear();
//                        durationTs.clear();
//                        timerList.clear();
                    }
                })
                .print();
        env.execute();
    }
}

package com.gf.day04;

import com.gf.utils.SensorReading;
import com.gf.utils.SensorSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

//连续1秒钟温度上升的监测
//使用1,2,3,4,5,6这个例子来分析
public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SensorSource())
                .keyBy(r->r.sensorId)
                .process(new Alert())
                .setParallelism(3)
                .print();

        env.execute();
    }

    public static class  Alert extends KeyedProcessFunction<String, SensorReading,String> {

        //用来保存最近一次的温度
        private ValueState<Integer> lastTemp;
        //用来保存报警定时器的时间戳
        private ValueState<Long> timerTs;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //初始化一张hash
            //key -> Integer
            lastTemp= getRuntimeContext().getState(
                    new ValueStateDescriptor<Integer>("last-temp", Types.INT)
            );
            //初始化一张hash
            //key ->Long
            timerTs= getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("timer",Types.LONG)
            );
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            System.out.println("process的并行子任务索引:"+getRuntimeContext().getIndexOfThisSubtask()+
            "处理的数据为"+ctx.getCurrentKey()+"value为："+value.temperature);
            Integer prevTemp = null;
            if (lastTemp.value()!=null) {
                //说明来的不是第一条温度
                //将lastTemp里面的值缓存到prevTemp中
                prevTemp=lastTemp.value();
            }
            //将当前温度保存到lastTemp中
            lastTemp.update(value.temperature);


            if (prevTemp ==null) {
                long oneSecLater = ctx.timerService().currentProcessingTime() + 1000L;
                ctx.timerService().registerProcessingTimeTimer(oneSecLater);
                timerTs.update(oneSecLater);
            }
            Long ts =null;
            if (timerTs.value()!=null) {
                //说明报警定时器存在
                ts = timerTs.value();
            }
            //处理逻辑
            //保证来的数据不是第一条温度值
            if (prevTemp!=null) {
                if(value.temperature< prevTemp && ts !=null){
                    //如果温度出现下降，且存在报警定时器
                    //1.将报警定时器删除
                    ctx.timerService().deleteProcessingTimeTimer(ts);
                    //2.将保存报警定时器时间戳的状态变量清空
                    timerTs.clear();
                }else if(value.temperature> prevTemp && ts ==null){
                    //如果出现温度上升，且不存在报警定时器
                    long oneSecondLater = ctx.timerService().currentProcessingTime() + 1000L;
                    //1.注册报警定时器
                    ctx.timerService().registerProcessingTimeTimer(oneSecondLater);
                    //2.将报警定时器时间戳保存下来
                    timerTs.update(oneSecondLater);
                }
            }

        }
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("传感器:"+ctx.getCurrentKey()+"连续1s温度上升!");
            //清空保存报警定时器时间戳的状态变量
            timerTs.clear();
        }
    }
}

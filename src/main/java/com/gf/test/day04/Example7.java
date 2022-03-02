package com.gf.test.day04;

import com.gf.utils.ClickEvent;
import com.gf.utils.ClickSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashMap;

public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r->r.url)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new CountAgg(),new WindowResult())
                .print();


        env.execute();
    }
    /**
     *  AggregateFunction<ClickEvent,Long,Long> : 输入，累加器，输出
     *  AggregateFunction:累加器的数据类型可以是Long,List,Map...等类型
     */
    public static class  CountAgg implements AggregateFunction<ClickEvent, Long,Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ClickEvent value, Long accumulator) {
            return accumulator+1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }
    //ProcessWindowFunction<Long,String,String, TimeWindow>: IN,OUT,KEY,Window
    public static class  WindowResult extends ProcessWindowFunction<Long,String,String,TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            out.collect("url"+key+"在窗口"+
                    new Timestamp(context.window().getStart())+"~"+
                    new Timestamp(context.window().getEnd())+"访问量为"+
                    elements.iterator().next());
        }
    }
}

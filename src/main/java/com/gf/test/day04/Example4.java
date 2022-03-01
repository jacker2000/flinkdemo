package com.gf.test.day04;

import com.gf.day04.Example5;
import com.gf.utils.ClickEvent;
import com.gf.utils.ClickSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .keyBy(r->r.url)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new  WindowResult())
                .print();
        env.execute();
    }

    public static class WindowResult extends ProcessWindowFunction<ClickEvent,String,String, TimeWindow>{

        @Override
        public void process(String key, Context context, Iterable<ClickEvent> elements, Collector<String> out) throws Exception {

            //elements.spliterator().getExactSizeIfKnown()) 获取迭代器中元素数量
            out.collect("url"+key +"在窗口"+
                    new Timestamp(context.window().getStart())+"~"+
                    new Timestamp(context.window().getEnd())+"中访问量是:"+
                    elements.spliterator().getExactSizeIfKnown());
        }
    }

}

package com.gf.day04;

import com.gf.utils.ClickEvent;
import com.gf.utils.ClickSource;
import com.gf.utils.UrlViewCountPerWindow;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 *  每个url在每隔5秒滚动窗口中被浏览次数
 *  增量聚合函数和全窗口函数结合使用
 */
public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r->r.url)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new ContAgg(),new WindowResult())
                .print();
        env.execute();
    }

    //输入的泛型是 AggregateFunction输出泛型
    public static class WindowResult extends ProcessWindowFunction<Long,UrlViewCountPerWindow,String,TimeWindow>{

        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<UrlViewCountPerWindow> out) throws Exception {
            //注意：迭代器只有一个元素,窗口闭合时，getResult返回值
            out.collect(
                    new UrlViewCountPerWindow(
                            key,
                            elements.iterator().next(),
                            context.window().getStart(),
                            context.window().getEnd()
                    )
            );
        }
    }

    public static class ContAgg implements AggregateFunction<ClickEvent, Long, Long> {
        //创建空累加器
        @Override
        public Long createAccumulator() {
            return 0L;
        }
        //定义输入数据和累加器的聚合规则，并返回新的累加器
        @Override
        public Long add(ClickEvent value,Long accumulator){
            return  accumulator+1L;
        }

        //窗口闭合，输出结果
        //将getResult返回值发送给ProcessWindowFunction
        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }
        @Override
        public Long merge(Long a,Long b){
            return null;
        }

    }
}

package com.gf.day04;

import com.gf.utils.ClickEvent;
import com.gf.utils.ClickSource;
import com.gf.utils.UrlViewCountPerWindow;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 *  每个url在每隔5秒滚动窗口中被浏览次数
 */
public class FlinkTumblingProcessingTimeWindows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r->r.url)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<ClickEvent, UrlViewCountPerWindow, String, TimeWindow>() {

                    /*
                       时间到达窗口接收时间-1毫秒时，触发process执行
                     */
                    @Override
                    public void process(String key, Context context, Iterable<ClickEvent> elements, Collector<UrlViewCountPerWindow> out) throws Exception {
                        //迭代器中包含窗口中所有元素
                        //为了计算url访问量，将窗口所有数据都保存下来
                        long count =0L;
                        for (ClickEvent e : elements) {
                            count++;
                        }
                        out.collect(new UrlViewCountPerWindow(
                                key,
                                count,
                                context.window().getStart(),
                                context.window().getEnd()
                        ));
                    }
                })
                .print();
        env.execute();
    }
}

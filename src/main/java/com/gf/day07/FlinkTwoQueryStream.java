package com.gf.day07;

import com.gf.utils.ClickEvent;
import com.gf.utils.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * 实现流的
 */
public class FlinkTwoQueryStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<ClickEvent> clickSource = env.addSource(new ClickSource());
        //查询字符串的流
        DataStreamSource<String> queryStream = env.socketTextStream("localhost", 9999);
        clickSource.keyBy(r -> r.url)
                //将queryStream广播之前，要保证queryStream的并行度是1
                //也就是要将数据按顺序广播出去
                .connect(queryStream.setParallelism(1).broadcast())
                .flatMap(new Query())
                .print();

        env.execute();
    }

    //in1(第一条流的泛型) in2(第二条流的泛型) out
    public static class Query implements CoFlatMapFunction<ClickEvent, String, ClickEvent> {

        //全局变量用来保存查询字符串
        private String query = "";

        @Override
        public void flatMap1(ClickEvent value, Collector<ClickEvent> out) throws Exception {
            //逻辑判断
            if (value.username.equals(query)) {
                out.collect(value);
            }
        }

        @Override
        public void flatMap2(String value, Collector<ClickEvent> out) throws Exception {
            query = value;
        }
    }
}

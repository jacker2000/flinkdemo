package com.gf.day02.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class FlinkTransformTuple {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件中读取数据,类型是String
        DataStreamSource<String> source =
                env.readTextFile("D:\\IdeaProjects\\bigdataCode\\flink_0906\\src\\main\\resources\\words.txt");
        //将字符串切割并转换成元组，"hello world" ->("hello",1),("world",1)

        SingleOutputStreamOperator<Tuple2<String, Integer>> mappedStream = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] array = value.split(" ");
                for (String word : array) {
                    //收集并发送数据
                    out.collect(Tuple2.of(word, 1));
                }
            }
        })
                .returns(Types.TUPLE(Types.STRING, Types.INT));
        //指定key
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = mappedStream.keyBy(r -> r.f0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum("f1");
        result.print();

        env.execute();
    }
}

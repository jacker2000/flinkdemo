package com.gf.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
/**
 *  内存方式:
 *   从socket消费数据，并计算word count
 *
 *   windows:
 *      发送nc(netcat)命令：
 *           .\nc.exe -lp 9999
 */

public class FlinkWorldCount1 {
    //主程序execute抛出异常！
    public static void main(String[] args) throws Exception {

        //1.创建应用程序的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         *  todo
         *      2.设置并行任务的数量1
         *         申请1个任务插槽来执行程序
         */
        env.setParallelism(1);
        //从socket读取数据，类型是String
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        //将字符串切割并转换成元组，"hello world" ->("hello",1),("world",1)
        //map操作
        //使用flatMap算子
        SingleOutputStreamOperator<Tuple2<String, Integer>> mappedStream = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            /**
             *  1.使用匿名类的方式实现flatMap计算逻辑
             *  2.第一个泛型表示输入的类型,也就是source输出的类型
             *  3.第二个泛型是flatmap算子输出的类型
             * @param value
             * @param out
             * @throws Exception
             */

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                //第一个参数是输入数据
                //第二个参数是集合，用来收集并发送将要输出的数据
                String[] array = value.split(" ");
                for (String word : array) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });
        //shuffle操作
        //f0表示元组的第0个字段
        //r-->r.f0表示为每一条输入的数据指定一个key
        //将数据路由到key对应的逻辑分区做聚合
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = mappedStream.keyBy(r -> r.f0);
        //针对相同key的f1字段做聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum("f1");
        result.print();

        //提交并执行程序
        env.execute();

    }
}

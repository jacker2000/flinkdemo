package com.gf.day02.physicalpartition;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkBroadcast {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        //全局
        env.setParallelism(1);

        env
                .fromElements(1,2,3)
                .broadcast() //把数据复制4份，分别发到不同的插槽中
                .print("broadcast:").setParallelism(4);

        env
                .fromElements(1,2,3)
                /**
                 *  shuffle维护的key随机,维护的value也是随机的
                 */
                .shuffle()
                .print("shuffle随机发送").setParallelism(5);

       env
        .fromElements(1,2,3,4,5)
       /**
        * todo
        *   轮询方式
        *     rebalance对维护的分区key进行轮询(遍历)，对维护的value进行轮询(遍历)
        */
        .rebalance()
        .print("round-robin:").setParallelism(2);

        env
                .fromElements(1,2,3,4)
                /**
                 *  todo
                 *      global:
                 *          把数据全部发送到第一个分区中 相当于 r->1
                 */
                .global()
                .print("global").setParallelism(3);
        env.execute();
    }
}

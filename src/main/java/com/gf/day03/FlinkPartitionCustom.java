package com.gf.day03;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkPartitionCustom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(1, 2, 3, 4, 5, 6, 7, 8).setParallelism(1)
                .partitionCustom(
                        //指定将某个key的数据发送到哪一个物理分区
                        new Partitioner<Integer>() {
                            @Override
                            public int partition(Integer key, int numPartitions) {
                                /**
                                 *  todo
                                 *      参数key是下面的KeySelector指定的key
                                 *      将key为1的发送到下游的第一个分区
                                 *      将key为0的发送到下游的第0个分区
                                 *      返回值是数据要发送到下游的物理分区的索引
                                 *    如果不自定义物理分区，直接通过keyBy进行逻辑分区的话
                                 *      程序会自动去做调度，自定义物理分区相当于手动调度的方式
                                 *      而如果直接通过keyBy进行逻辑分区，自己是无法决定数据到哪个区，程序是自己调度的
                                 */

                                return key==1?0:1;
                            }
                        },
                        //为每个数指定key
                        //每来一条数据就指定一个key,那么这条数据发送到下游哪一个分区呢
                        new KeySelector<Integer, Integer>() {
                            @Override
                            public Integer getKey(Integer value) throws Exception {
                                //按照奇偶性来逻辑分区
                                return value % 2;
                            }
                        })
                .print("自定义物理分区").setParallelism(2);

        env.execute();
    }
}

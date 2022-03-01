package com.gf.day01;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkWorldCount3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        //全局
        env.setParallelism(1);

        /**
         *  todo
         *      算子的优先级比全局的优先级要高
         *      fromElements：
         *          占用1个任务插槽
         *     sum:
         *        4个并行子任务
         *     print:
         *       4个并行子任务
         *    sum和print 一定共享一个任务插槽
         *    不同算子可以共享一个任务插槽
         *
         *      相同算子的不同并行子任务，必须放在不同任务插槽
         *      如果算子fromElements不设置并行度，则继承全局并行度1
         *  todo
         *      数据流程:
         *        (1,2,3,4,5,6,7,8)  r%3
         *        1%3 -->1(位置)
         *          1 (值)
         *        2%3 -->2(位置)
         *          2 (值)
         *        3%3 -->0(位置)
         *          初始值3(值)
         *        4%3 -->1(位置)
         *         上一个1+4=5(值)
         *        5%3 -->2(位置)
         *         2+5=7(值)
         *        6%3-->0(位置)
         *        3+6 =9(值)
         *        7%3-->1(位置)
         *        5+7=12(值)
         *        8%3-->2(位置)
         *        7+8=15(值)
         */
        env.fromElements(1,2,3,4,5,6,7,8).setParallelism(1)
                /**
                 *  理论 keyby 提供3个任务插槽(0,1,2)，实际用了2个插槽，算法是通过key取hash取模的方式路由到对应分区上
                 */
                .keyBy(r->r%3)
                .sum(0).setParallelism(4)
                .print("数据之和").setParallelism(4);


        env
                .fromElements(1,2,3,4,5,6,7,8)
                .keyBy(r->1)
                .max(0)
                .print("数据最大值:");
        env.execute();


    }
}

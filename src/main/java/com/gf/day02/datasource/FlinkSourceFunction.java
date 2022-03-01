package com.gf.day02.datasource;

import com.gf.utils.ClickSource;
import com.gf.utils.IntegerSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *  并行度是1的数据源：
 *      sourceFunction,
 */
public class FlinkSourceFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /**
         *  todo
         *      .print():
         *          调用toString方法，输出到stout标准输出里面，文件描述符为1的文件，sout2为标准错误，到控制台，
         *              标准输入是0，标准输出是1
         */
        env.addSource(new ClickSource()).print();
        env.addSource(new IntegerSource()).print();
        /**
         *  todo
         *      两个source 是交错执行
         *          同一个任务插槽，但是是两个线程，两个线程执行顺序是不确定的
         *          每个线程中的addSource 和print 是两个顶点，合成一个任务链(并行度相同,没有shuffle)
         */
        env.execute();
    }
}

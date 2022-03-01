package com.gf.day02.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlinkTransformFlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                //fromElements(T ...)- 从给定的对象序列创建数据流。所有对象必须属于同一类型。
                .fromElements("white","black","gray")
                //第一个参数是输入，第二个参数是输出，声明输出类型，做了强制类型转换，所以不用做注解
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        if (s.equals("white")) {
                            collector.collect(s);
                        }else if(s.equals("black")){
                            collector.collect(s);
                        }
                    }
                }).print();

        env
                .fromElements("white","black","gray")
                /**
                 *  todo
                 *      flatMap:
                 *          第一个参数是参数列表
                 *          第二个参数是函数体
                 *      输出：
                 *          如果不指定返回类型，则报错
                 *          InvalidTypesException: The return type of function 'main(FlinkTransformFlatMap.java:34)'
                 *              could not be determined automatically, due to type erasure.
                 *              You can give type information hints by using the returns(...) method on the result of the transformation call,
                 *              or by letting your function implement the 'ResultTypeQueryable' interface.
                 *          由于类型擦除，无法自动确定。
                 *              你可以通过使用returns(…)方法来给出类型信息提示，或者让你的函数实现'ResultTypeQueryable'接口。
                 */
                .flatMap((String s, Collector<String> collector)->{
                    if (s.equals("white")) {
                        collector.collect(s);
                    }else if(s.equals("black")){
                        collector.collect(s);
                    }
                })
                /**
                 *  todo
                 *      Java在编译字节码时，做泛型擦除
                 *      Collector<String> 被擦除为Collector<Object>
                 *      对FlatMap输出类型做注解
                 *      什么时候做数据标注：
                 *          基本类型不用标注
                 *              map匿名函数不用标注，因为是基本类型String
                 *          返回值不是java基本类型的时候(即泛型时)需要做标注
                 *            如Pojo类，元组
                 */
                .returns(Types.STRING )
                .print();
        env.execute();
    }
}

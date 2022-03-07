package com.gf.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Example8 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //获取表执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, settings);

        streamTableEnvironment.executeSql("CREATE TABLE clicks (`user` STRING, `url` STRING) " +
                "WITH (" + "'connector' = 'filesystem'," + "'path' = 'D:\\IdeaProjects\\bigdataCode\\flink_0906\\src\\main\\resources\\file.csv'," + "'format' = 'csv')");

        // 定义输出表，连接到标准输出
        streamTableEnvironment
                .executeSql("CREATE TABLE ResultTable (`user` STRING, `cnt` BIGINT) " +
                        "WITH ('connector' = 'print')");
// 在输出表上进行查询，查询结果写入输出表
        streamTableEnvironment
                .executeSql("INSERT INTO ResultTable SELECT user, COUNT(url) as cnt FROM clicks GROUP BY user");
        //默认Executor
    }
}

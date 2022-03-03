package com.gf.day06;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

//sink  to kafka
public class FlinkKafkaProducerSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.setProperty("group.id", "consumer-group");
        env
                .readTextFile("D:\\IdeaProjects\\bigdataCode\\flink_0906\\src\\main\\resources\\UserBehavior.csv")
                .addSink(new FlinkKafkaProducer<String>(
                        "userbehavior",
                        new SimpleStringSchema(),
                        properties
                ));
        env.execute();
    }
}

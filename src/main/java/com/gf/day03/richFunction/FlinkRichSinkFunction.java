package com.gf.day03.richFunction;

import com.gf.utils.ClickEvent;
import com.gf.utils.ClickSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 自定义输出
 * sink to mysql
 * create database userbehavior;
 * create table clicks (username,url)
 */
public class FlinkRichSinkFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .addSink(new SinkToMysql());
        env.execute();
    }

    /**
     * RichSinkFunction的泛型是输入数据的泛型
     * 自己实现flink和mysql的连接器
     */
    public static class SinkToMysql extends RichSinkFunction<ClickEvent> {
        private Connection conn;
        private PreparedStatement insertStmt;
        private PreparedStatement updateStmt;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            conn = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/userbehavior?useSSL=false",
                    "root",
                    "root"
            );
            insertStmt = conn.prepareStatement("INSERT INTO clicks (username,url) VALUES (?,?)");
            updateStmt = conn.prepareStatement("UPDATE clicks SET url = ? WHERE username = ?");
        }

        //每来一条数据，触发一次调用
        @Override
        public void invoke(ClickEvent value, Context context) throws Exception {
            updateStmt.setString(1, value.url);
            updateStmt.setString(2, value.username);
            updateStmt.execute();
            //更新的行数为0，说明没有username这一行
            //幂等性写入mysql
            if (updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(1, value.username);
                insertStmt.setString(2, value.url);
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            insertStmt.close();
            updateStmt.close();
            conn.close();
        }
    }
}

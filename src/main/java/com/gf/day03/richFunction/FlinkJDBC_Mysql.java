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
 *  富函数发送：
 *      JDBC连接
 */
public class FlinkJDBC_Mysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .addSink(new getMysql()); //这里的sink和print效果都是一样
        env.execute();
    }


    public static class getMysql extends RichSinkFunction<ClickEvent> {
        private Connection conn;
        private PreparedStatement insertStmt;
        private PreparedStatement updateStmt;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            conn = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/userbehavior?userSSL=false",
                    "root",
                    "root"
            );
            insertStmt = conn.prepareStatement("INSERT INTO clicks (username,url) VALUES (?,?)");
            updateStmt = conn.prepareStatement("UPDATE clicks SET url=? where username=? ");
        }

        @Override
        public void invoke(ClickEvent value, Context context) throws Exception {
            //幂等写入
            updateStmt.setString(1,value.url);
            updateStmt.setString(2,value.username);
            updateStmt.execute();

            if (updateStmt.getUpdateCount()==0) {
                insertStmt.setString(1,value.username);
                insertStmt.setString(2,value.url);
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            conn.close();
        }

    }
}

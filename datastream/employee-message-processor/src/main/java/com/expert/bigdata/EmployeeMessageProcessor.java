package com.expert.bigdata;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import static org.apache.flink.table.api.Expressions.$;


public class EmployeeMessageProcessor {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建Kafka source表
        tableEnv.executeSql(
                "CREATE TABLE employee_source (\n" +
                        "  id BIGINT,\n" +
                        "  name STRING,\n" +
                        "  department STRING,\n" +
                        "  salary DOUBLE,\n" +
                        "  message_time TIMESTAMP(3),\n" +
                        "  WATERMARK FOR message_time AS message_time\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'employee-topic1',\n" +
                        "  'properties.bootstrap.servers' = 'kafka:9092',\n" +
                        "  'properties.group.id' = 'employee-group',\n" +
                        "  'scan.startup.mode' = 'latest-offset',\n" +
                        "  'format' = 'json'\n" +
                        ")"
        );

        // 创建MySQL sink表
        tableEnv.executeSql(
                "CREATE TABLE employee_mysql_sink (\n" +
                        "  id BIGINT,\n" +
                        "  name STRING,\n" +
                        "  department STRING,\n" +
                        "  salary DOUBLE,\n" +
                        "  message_time TIMESTAMP(3),\n" +
                        "  process_time AS PROCTIME(),\n" +
                        "  PRIMARY KEY (id) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        "  'connector' = 'jdbc',\n" +
                        "  'url' = 'jdbc:mysql://mysql:3306/streampark',\n" +
                        "  'table-name' = 'employee_messages',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = 'streampark',\n" +
                        "  'driver' = 'com.mysql.cj.jdbc.Driver'\n" +
                        ")"
        );

        // 创建Kafka sink表
        tableEnv.executeSql(
                "CREATE TABLE employee_sink (\n" +
                        "  id BIGINT,\n" +
                        "  name STRING,\n" +
                        "  department STRING,\n" +
                        "  salary DOUBLE,\n" +
                        "  message_time TIMESTAMP(3),\n" +
                        "  process_time AS PROCTIME(),\n" +
                        "  PRIMARY KEY (id) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'employee-topic2',\n" +
                        "  'properties.bootstrap.servers' = 'kafka:9092',\n" +
                        "  'format' = 'json',\n" +
                        "  'sink.partitioner' = 'round-robin'\n" +
                        ")"
        );

        // 处理数据：添加当前时间戳
        Table processedTable = tableEnv.from("employee_source")
                .select(
                        $("id"),
                        $("name"),
                        $("department"),
                        $("salary"),
                        $("message_time")
                );

        // 写入MySQL
        processedTable.executeInsert("employee_mysql_sink");

        // 写入Kafka
        processedTable.executeInsert("employee_sink");

        // 执行任务
        env.execute("Employee Message Processor");
    }
}
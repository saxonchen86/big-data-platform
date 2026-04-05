package com.bigdata.common.utils;

import org.apache.flink.api.java.utils.ParameterTool;

public class MyParameter {

    private String kafkaBootstrapServers;
    private String dbUrl;
    private String dbUsername;
    private String dbPassword;
    private String sourceTopic;
    private String sinkTopic;
    private String sinkTablename;

    public MyParameter(ParameterTool parameterTool) {
        this.kafkaBootstrapServers = parameterTool.get("kafka.bootstrap.servers", "localhost:9092");
        this.dbUrl = parameterTool.get("dbUrl", "jdbc:mysql://localhost:3306/streampark");
        this.dbUsername = parameterTool.get("dbUsername", "root");
        this.dbPassword = parameterTool.get("dbPassword", "streampark");
        this.sourceTopic = parameterTool.get("sourceTopic", "topic1");
        this.sinkTopic = parameterTool.get("sinkTopic", "topic2");
        this.sinkTablename = parameterTool.get("sinkTablename");
    }

    // Getter methods
    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public String getDbUrl() {
        return dbUrl;
    }

    public String getDbUsername() {
        return dbUsername;
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public String getSourceTopic() {
        return sourceTopic;
    }

    public String getSinkTopic() {
        return sinkTopic;
    }

    public String getSinkTablename() {
        return sinkTablename;
    }

    // Setter methods
    public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
    }

    public void setDbUrl(String dbUrl) {
        this.dbUrl = dbUrl;
    }

    public void setDbUsername(String dbUsername) {
        this.dbUsername = dbUsername;
    }

    public void setDbPassword(String dbPassword) {
        this.dbPassword = dbPassword;
    }

    public void setSourceTopic(String sourceTopic) {
        this.sourceTopic = sourceTopic;
    }

    public void setSinkTopic(String sinkTopic) {
        this.sinkTopic = sinkTopic;
    }

    public void setSinkTablename(String sinkTablename) {
        this.sinkTablename = sinkTablename;
    }
}

package com.expert.bigdata.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.common.utils.MyParameter;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.vector.request.InsertReq;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class EthSentimentVectorDatabaseSink extends RichSinkFunction<String> {
    private transient MilvusClientV2 client;

    @Override
    public void open(Configuration parameters) {
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        MyParameter myParameter = new MyParameter(params);
        String host = myParameter.getMilvusHost();
        this.client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://" + host + ":19530").build());
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        // 1. 解析上游传来的混合了情绪分析和向量数据的 JSON
        JSONObject node = JSON.parseObject(value);

        JSONObject row = new JSONObject();
        // 标量字段存入
        row.put("raw_content", node.getString("raw_content"));
        row.put("sentiment_score", node.getInteger("score")); // 存入情绪分数标量
        row.put("sentiment_reason", node.getString("reason"));
        row.put("timestamp", node.getLong("timestamp"));

        // 2. 提取并转换向量数组
        JSONArray vectorArray = node.getJSONArray("vector");
        List<Float> vector = new ArrayList<>(vectorArray.size());
        for (int i = 0; i < vectorArray.size(); i++) {
            vector.add(vectorArray.getFloat(i));
        }
        row.put("vector", vector);

        // 3. 写入针对情绪分析调整后的集合
        InsertReq insertReq = InsertReq.builder()
                .collectionName("eth_sentiment_analysis") // 调整后的 collectionName
                .data(Collections.singletonList(row))
                .build();

        client.insert(insertReq);
        System.out.println("🚀 ETH 情绪数据及向量已持久化：Score=" + node.getInteger("score"));
    }

    @Override
    public void close() throws InterruptedException {
        if (client != null) {
            client.close(3000);
        }
    }
}
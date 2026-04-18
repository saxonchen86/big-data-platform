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
import org.apache.flink.streaming.api.functions.sink.SinkFunction; // 只要这个 Flink 的包
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

    // 【修复点 2：明确使用 SinkFunction.Context】
    @Override
    public void invoke(String value, SinkFunction.Context context) throws Exception {
        // 1. 解析上游传来的混合了情绪分析和向量数据的 JSON
        JSONObject node = JSON.parseObject(value);

        JSONObject row = new JSONObject();
        // 基础标量字段存入
        row.put("raw_content", node.getString("raw_content"));
        row.put("sentiment_score", node.getInteger("score")); // 存入情绪分数标量
        row.put("sentiment_reason", node.getString("reason"));
        row.put("timestamp", node.getLong("timestamp"));

        // 【架构升级点：写入默认的回测结算字段】
        // 保证 Milvus 插入不报 Schema 不匹配的错误，并交给 Python 脚本去收割
        // 这里 price_at_t 可以用 0.0 占位，或者你以后在上游算子把实时价格查好传过来
        row.put("price_at_t", node.getFloatValue("price_at_t"));
        row.put("price_after_24h", 0.0f);
        row.put("return_24h", 0.0f);
        row.put("is_settled", false); // 标记为未结算，等待 24 小时后 Python 脚本更新

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
            client.close(3000); // 确保优雅关闭连接
        }
    }
}
package com.expert.bigdata.func;


import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.vector.request.InsertReq;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class VectorDatabaseSink extends RichSinkFunction<String> {
    private transient MilvusClientV2 client;
    private transient ObjectMapper mapper;

    @Override
    public void open(Configuration parameters) {
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String host = params.get("milvusHost", "localhost");

        this.client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://" + host + ":19530").build());
        this.mapper = new ObjectMapper();
    }

    // 修改后的代码片段
    @Override
    public void invoke(String value, Context context) throws Exception {
        JsonNode node = mapper.readTree(value);
        JSONObject row = new JSONObject();
        row.put("raw_log", node.get("raw_log").asText());

        List<Float> vector = new ArrayList<>();
        node.get("vector").forEach(v -> vector.add(v.floatValue()));
        row.put("vector", vector);

        // 🌟 核心修改：将 UpsertReq 改为 InsertReq，将 client.upsert 改为 client.insert
        InsertReq insertReq = InsertReq.builder()
                .collectionName("dofi_realtime_knowledge")
                .data(Collections.singletonList(row))
                .build();

        client.insert(insertReq); // 使用 insert
        System.out.println("🔥 成功持久化一条实时语义数据！");
    }

    @Override
    public void close() {
        if (client != null) {
            // 传入一个超时时间（毫秒），例如 3000ms
            // 如果你的 SDK 版本还是报错，可以尝试 client.close(3000L);
            try {
                client.close(3000);
            } catch (Exception e) {
                System.err.println("关闭 Milvus 连接时发生异常: " + e.getMessage());
            }
        }
    }
}
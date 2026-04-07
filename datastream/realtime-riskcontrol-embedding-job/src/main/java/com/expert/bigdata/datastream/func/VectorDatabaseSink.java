package com.expert.bigdata.datastream.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
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

    @Override
    public void open(Configuration parameters) {
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String host = params.get("milvusHost", "localhost");

        this.client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://" + host + ":19530").build());
        // 🗑️ 删除了沉重的 ObjectMapper 初始化
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        // 1. 直接使用 Fastjson 解析原始字符串
        JSONObject node = JSON.parseObject(value);

        JSONObject row = new JSONObject();
        row.put("raw_log", node.getString("raw_log"));

        // 2. 提取向量数组
        JSONArray vectorArray = node.getJSONArray("vector");

        // 🌟 性能优化：直接指定 ArrayList 的初始容量
        // 在 Flink 高频流处理中，避免 ArrayList 动态扩容带来的 CPU 和 GC 开销
        List<Float> vector = new ArrayList<>(vectorArray.size());
        for (int i = 0; i < vectorArray.size(); i++) {
            vector.add(vectorArray.getFloat(i));
        }
        row.put("vector", vector);

        InsertReq insertReq = InsertReq.builder()
                .collectionName("dofi_realtime_knowledge")
                .data(Collections.singletonList(row))
                .build();

        client.insert(insertReq);
        System.out.println("🔥 成功持久化一条实时语义数据！");
    }

    @Override
    public void close() {
        if (client != null) {
            try {
                client.close(3000);
            } catch (Exception e) {
                System.err.println("关闭 Milvus 连接时发生异常: " + e.getMessage());
            }
        }
    }
}
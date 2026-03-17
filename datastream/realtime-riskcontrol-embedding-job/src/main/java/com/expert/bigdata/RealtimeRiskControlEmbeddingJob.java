package com.expert.bigdata;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.vector.request.InsertReq;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class RealtimeRiskControlEmbeddingJob {

    public static void main(String[] args) throws Exception {
        // 1. 初始化环境并解析自适应参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params); // 关键：全局共享参数
        env.enableCheckpointing(5000);

        // 2. Kafka Source (使用外部监听端口 9092)
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("risk_control_logs")
                .setGroupId("dofi-group-v3")
                .setStartingOffsets(OffsetsInitializer.latest()) // 确保能读到旧数据
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 3. 异步向量化算子
        DataStream<String> embeddedStream = AsyncDataStream.unorderedWait(
                kafkaStream,
                new OllamaAsyncEmbeddingFunction(),
                10000, TimeUnit.MILLISECONDS,
                100
        );

        // 4. 写入 Milvus Sink (必须确保 addSink 被正确调用)
        embeddedStream.addSink(new VectorDatabaseSink()).name("Milvus-Sink");

        // 5. 启动任务
        env.execute("Dofi-Realtime-AI-Pipeline");
    }

    public static class OllamaAsyncEmbeddingFunction extends RichAsyncFunction<String, String> {
        private transient HttpClient client;
        private transient ObjectMapper mapper;
        private String ollamaUrl;

        @Override
        public void open(Configuration parameters) {
            ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            // 自动识别环境：本地跑用 localhost，容器跑传参数 --ollama.host host.docker.internal
            String host = params.get("ollama.host", "localhost");
            this.ollamaUrl = "http://" + host + ":11434/api/embeddings";

            this.client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
            this.mapper = new ObjectMapper();
        }

        @Override
        public void asyncInvoke(String input, ResultFuture<String> resultFuture) {
            try {
                Map<String, String> body = new HashMap<>();
                body.put("model", "nomic-embed-text");
                body.put("prompt", input);

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(ollamaUrl))
                        .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)))
                        .build();

                client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                        .thenAccept(resp -> {
                            try {
                                JsonNode node = mapper.readTree(resp.body());
                                String res = String.format("{\"raw_log\": \"%s\", \"vector\": %s}",
                                        input.replace("\"", "\\\""), node.get("embedding").toString());
                                resultFuture.complete(Collections.singletonList(res));
                            } catch (Exception e) { resultFuture.completeExceptionally(e); }
                        });
            } catch (Exception e) { resultFuture.completeExceptionally(e); }
        }
    }

    public static class VectorDatabaseSink extends RichSinkFunction<String> {
        private transient MilvusClientV2 client;
        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parameters) {
            ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            String host = params.get("milvus.host", "localhost");

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
}
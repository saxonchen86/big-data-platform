package com.expert.bigdata.datastream.func;

import com.bigdata.common.utils.MyParameter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class OllamaAsyncEmbeddingFunction extends RichAsyncFunction<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(OllamaAsyncEmbeddingFunction.class);
    private transient HttpClient client;
    private transient ObjectMapper mapper;
    private String ollamaUrl;

    @Override
    public void open(Configuration parameters) {
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        MyParameter myParameter = new MyParameter(params);
        // 确保容器内能解析 host.docker.internal
        String host = myParameter.getOllamaHost();
        this.ollamaUrl = "http://" + host + ":11434/api/embeddings";

        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5)) // 稍微增加一点连接宽容度
                .build();
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
                    .timeout(Duration.ofSeconds(20)) // HTTP 请求层面 20 秒超时
                    .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)))
                    .build();

            client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .whenComplete((resp, throwable) -> {
                        if (throwable != null) {
                            LOG.error("请求 Ollama 失败: {}", input, throwable);
                            // 不直接 completeExceptionally，而是丢弃或打入死信队列侧输出流
                            // 为了不阻断主流，这里传空集合表示放弃该条数据（生产中应输出到侧输出流保存）
                            resultFuture.complete(Collections.emptyList());
                            return;
                        }

                        try {
                            if (resp.statusCode() == 200) {
                                JsonNode node = mapper.readTree(resp.body());
                                if (node.has("embedding")) {
                                    String res = String.format("{\"raw_log\": \"%s\", \"vector\": %s}",
                                            input.replace("\"", "\\\""), node.get("embedding").toString());
                                    resultFuture.complete(Collections.singletonList(res));
                                } else {
                                    LOG.warn("Ollama 返回数据格式异常: {}", resp.body());
                                    resultFuture.complete(Collections.emptyList());
                                }
                            } else {
                                LOG.warn("Ollama 返回非 200 状态码: {}", resp.statusCode());
                                resultFuture.complete(Collections.emptyList());
                            }
                        } catch (Exception e) {
                            LOG.error("解析 Ollama 响应失败", e);
                            resultFuture.complete(Collections.emptyList());
                        }
                    });
        } catch (Exception e) {
            LOG.error("构建或发送 HTTP 请求时发生异常", e);
            resultFuture.complete(Collections.emptyList());
        }
    }

    /**
     * 核心防御机制：接管 Flink 的超时异常！
     * 超过 Flink 规定的 unorderedWait 时间（如 30 秒）时触发，防止 Job 崩溃。
     */
    @Override
    public void timeout(String input, ResultFuture<String> resultFuture) throws Exception {
        LOG.warn("❌ 数据处理超时 (超过了 Flink 设置的 Async Timeout) 丢弃数据: {}", input);
        // 不抛出异常，完成 future，让作业继续运行后续数据
        resultFuture.complete(Collections.emptyList());
    }
}
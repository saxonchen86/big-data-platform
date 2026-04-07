package com.expert.bigdata.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.common.utils.MyParameter;
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
import java.util.concurrent.Executors;

public class OllamaAsyncEmbeddingFunction extends RichAsyncFunction<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(OllamaAsyncEmbeddingFunction.class);
    private transient HttpClient client;
    private String ollamaUrl;

    @Override
    public void open(Configuration parameters) {
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        MyParameter myParameter = new MyParameter(params);
        String host = myParameter.getOllamaHost();
        this.ollamaUrl = "http://" + host + ":11434/api/embeddings";

        // 🌟 架构师视角：定制线程池
        // 原生 HttpClient 默认使用无界缓存线程池，在 Flink 高吞吐异步 IO 时可能导致线程爆炸
        // 这里显式指定一个有界或合理的线程池来控制并发资源消耗
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .executor(Executors.newFixedThreadPool(20)) // 根据 TaskManager 的 CPU 核心数调优
                .build();
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) {
        try {
            Map<String, String> body = new HashMap<>();
            body.put("model", "nomic-embed-text");
            body.put("prompt", input);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(ollamaUrl))
                    .timeout(Duration.ofSeconds(20))
                    // 1. 使用 Fastjson 序列化请求体
                    .POST(HttpRequest.BodyPublishers.ofString(JSON.toJSONString(body)))
                    .build();

            client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .whenComplete((resp, throwable) -> {
                        if (throwable != null) {
                            LOG.error("请求 Ollama 失败: {}", input, throwable);
                            resultFuture.complete(Collections.emptyList());
                            return;
                        }

                        try {
                            if (resp.statusCode() == 200) {
                                // 2. 使用 Fastjson 解析响应
                                JSONObject node = JSON.parseObject(resp.body());

                                if (node.containsKey("embedding")) {
                                    // 🌟 核心优化：拒绝手动拼接字符串，使用原生 JSON 对象构建
                                    // 彻底杜绝 input 中包含奇葩转义字符导致 JSON 格式损坏的风险
                                    JSONObject resultJson = new JSONObject();
                                    resultJson.put("raw_log", input);
                                    resultJson.put("vector", node.getJSONArray("embedding"));

                                    // 直接输出序列化后的安全字符串
                                    resultFuture.complete(Collections.singletonList(resultJson.toJSONString()));
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

    @Override
    public void timeout(String input, ResultFuture<String> resultFuture) throws Exception {
        LOG.warn("❌ 数据处理超时 (超过了 Flink 设置的 Async Timeout) 丢弃数据: {}", input);
        resultFuture.complete(Collections.emptyList());
    }
}
package com.expert.bigdata.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.common.utils.MyParameter;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.IOException;

public class EthEmbeddingFunction extends RichAsyncFunction<String, String> {
    private transient HttpClient client;
    private String ollamaUrl;
    private static final ExecutorService executor = Executors.newFixedThreadPool(20);

    @Override
    public void open(Configuration parameters) {
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        MyParameter myParameter = new MyParameter(params);
        this.ollamaUrl = "http://" + myParameter.getOllamaHost() + ":11434/api/embeddings";
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .executor(Executors.newFixedThreadPool(20))
                .build();
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) {
        // 解析上游情绪分析算子的输出
        JSONObject sentimentResult = JSON.parseObject(input);
        String textToEmbed = sentimentResult.getString("raw_content");
        JSONObject aiAnalysis = sentimentResult.getJSONObject("ai_analysis");

        Map<String, String> body = new HashMap<>();
        body.put("model", "nomic-embed-text");
        body.put("prompt", textToEmbed);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(ollamaUrl))
                .POST(HttpRequest.BodyPublishers.ofString(JSON.toJSONString(body)))
                .build();

        // 使用线程池优化异步调用
        CompletableFuture<HttpResponse<String>> future = CompletableFuture.supplyAsync(() -> {
            try {
                return client.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, executor);

        future.whenComplete((resp, throwable) -> {
            if (resp != null && resp.statusCode() == 200) {
                JSONObject node = JSON.parseObject(resp.body());

                // 构建合并后的结果给 Sink
                JSONObject output = new JSONObject();
                output.put("raw_content", textToEmbed);
                output.put("embedding", node.getJSONArray("embedding"));
                output.put("ai_analysis", aiAnalysis);

                resultFuture.complete(Collections.singletonList(output.toJSONString()));
            } else {
                // 处理错误情况
                resultFuture.completeExceptionally(
                        new RuntimeException("Failed to fetch embedding: " + (resp != null ? resp.statusCode() : "null response"))
                );
            }
        });
    }
}

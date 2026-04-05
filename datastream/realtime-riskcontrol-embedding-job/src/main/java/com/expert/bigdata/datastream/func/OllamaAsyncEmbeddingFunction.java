package com.expert.bigdata.datastream.func;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;

public class OllamaAsyncEmbeddingFunction extends RichAsyncFunction<String, String> {
    private transient HttpClient client;
    private transient ObjectMapper mapper;
    private String ollamaUrl;

    @Override
    public void open(Configuration parameters) {
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        // 自动识别环境：本地跑用 localhost，容器跑传参数 --ollama.host host.docker.internal
        String host = params.get("ollamaHost", "localhost");
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

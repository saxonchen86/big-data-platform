package com.expert.bigdata.func;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public class EthSentimentOllamaFunction extends RichAsyncFunction<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(EthSentimentOllamaFunction.class);
    private transient CloseableHttpAsyncClient httpClient;
    private final ObjectMapper mapper = new ObjectMapper();
    private String apiUrl;

    @Override
    public void open(Configuration parameters) {
        apiUrl = getRuntimeContext().getExecutionConfig().getGlobalJobParameters()
                .toMap().getOrDefault("ollama.api.url", "http://localhost:11434/api/generate");

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(5000)
                .setSocketTimeout(30000)
                .build();
        httpClient = HttpAsyncClients.custom().setDefaultRequestConfig(requestConfig).build();
        httpClient.start();
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
        JsonNode node = mapper.readTree(input);
        String summary = node.get("summary").asText();
        String traceId = node.has("id") ? node.get("id").asText() : "unknown";

        ObjectNode requestBody = mapper.createObjectNode();
        requestBody.put("model", "deepseek-r1:8b");
        requestBody.put("prompt", "Analyze the sentiment of this text for ETH trading. Output strictly JSON: {\"sentiment_score\": int(1-10), \"sentiment_reason\": string}. Text: " + summary);
        requestBody.put("stream", false);

        HttpPost request = new HttpPost(apiUrl);
        request.setEntity(new StringEntity(mapper.writeValueAsString(requestBody)));

        CompletableFuture.runAsync(() -> {
            try {
                httpClient.execute(request, new org.apache.http.concurrent.FutureCallback<HttpResponse>() {
                    @Override
                    public void completed(HttpResponse response) {
                        try {
                            String resJson = EntityUtils.toString(response.getEntity());
                            JsonNode resNode = mapper.readTree(resJson);
                            JsonNode contentJson = mapper.readTree(resNode.get("response").asText());

                            int score = contentJson.get("sentiment_score").asInt();
                            // 关键过滤逻辑：只保留极度悲观(<=2)或极度乐观(>=8)
                            if (score > 2 && score < 8) {
                                resultFuture.complete(Collections.emptyList());
                                return;
                            }

                            ObjectNode output = (ObjectNode) node;
                            output.put("sentiment_score", score);
                            output.put("sentiment_reason", contentJson.get("sentiment_reason").asText());
                            output.put("raw_content", summary);
                            output.put("sentiment_es", Instant.now().atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli());

                            resultFuture.complete(Collections.singletonList(mapper.writeValueAsString(output)));
                        } catch (Exception e) {
                            LOG.error("[{}] Sentiment analysis failed: {}", traceId, e.getMessage());
                            resultFuture.complete(Collections.emptyList());
                        }
                    }
                    @Override public void failed(Exception ex) { resultFuture.complete(Collections.emptyList()); }
                    @Override public void cancelled() { resultFuture.complete(Collections.emptyList()); }
                });
            } catch (Exception e) { resultFuture.complete(Collections.emptyList()); }
        });
    }

    @Override public void close() throws Exception { if (httpClient != null) httpClient.close(); }
}
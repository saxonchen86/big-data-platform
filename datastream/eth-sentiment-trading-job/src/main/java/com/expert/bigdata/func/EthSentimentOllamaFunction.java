package com.expert.bigdata.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
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
    private String apiUrl;

    @Override
    public void open(Configuration parameters) {
        // 核心修复：使用 host.docker.internal 并在获取参数时进行 trim() 防御
        apiUrl = getRuntimeContext().getExecutionConfig().getGlobalJobParameters()
                .toMap().getOrDefault("ollama.api.url", "http://host.docker.internal:11434/api/generate").trim();

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(5000)
                // 30s超时针对本地 30b/32b 大模型生成推理过程比较合理
                .setSocketTimeout(30000)
                .build();
        httpClient = HttpAsyncClients.custom().setDefaultRequestConfig(requestConfig).build();
        httpClient.start();
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
        JSONObject node = JSON.parseObject(input);
        String summary = node.getString("summary");
        String traceId = node.containsKey("id") ? node.getString("id") : "unknown";

        JSONObject requestBody = new JSONObject();
        // 匹配你本地的大模型
        requestBody.put("model", "gemma-31b-crack");
        requestBody.put("stream", false);

        // 1. Prompt 升级：增加思维链 (CoT) 约束
        String prompt = "You are an expert ETH quantitative analyst. Analyze the following text.\n" +
                "Step 1: Write down your step-by-step logical reasoning regarding market demand, liquidity, or macro impacts.\n" +
                "Step 2: Output a sentiment_score from 1 (Extreme Bearish) to 10 (Extreme Bullish).\n" +
                "Step 3: Provide a concise sentiment_reason.\n\n" +
                "Text: " + summary;
        requestBody.put("prompt", prompt);

        // 2. 确定性控制：Temperature 与 Top_P 组合
        JSONObject options = new JSONObject();
        options.put("temperature", 0.0);
        options.put("top_p", 0.5);
        requestBody.put("options", options);

        // 3. 架构控制：通过 JSON Schema 严格约束 Structured Outputs
        JSONObject formatNode = new JSONObject();
        formatNode.put("type", "object");

        JSONObject properties = new JSONObject();

        JSONObject reasoningProp = new JSONObject();
        reasoningProp.put("type", "string");
        properties.put("reasoning", reasoningProp);

        JSONObject scoreProp = new JSONObject();
        scoreProp.put("type", "integer");
        properties.put("sentiment_score", scoreProp);

        JSONObject reasonProp = new JSONObject();
        reasonProp.put("type", "string");
        properties.put("sentiment_reason", reasonProp);

        formatNode.put("properties", properties);

        JSONArray requiredNode = new JSONArray();
        requiredNode.add("reasoning");
        requiredNode.add("sentiment_score");
        requiredNode.add("sentiment_reason");
        formatNode.put("required", requiredNode);

        requestBody.put("format", formatNode);

        HttpPost request = new HttpPost(apiUrl);
        request.setHeader("Content-Type", "application/json");
        request.setEntity(new StringEntity(requestBody.toJSONString(), "UTF-8"));

        CompletableFuture.runAsync(() -> {
            try {
                httpClient.execute(request, new org.apache.http.concurrent.FutureCallback<HttpResponse>() {
                    @Override
                    public void completed(HttpResponse response) {
                        try {
                            String resJson = EntityUtils.toString(response.getEntity());
                            JSONObject resNode = JSON.parseObject(resJson);
                            String contentStr = resNode.getString("response");

                            // 4. 防御性编程：处理模型输出残留问题
                            int jsonStartIndex = contentStr.indexOf("{");
                            if (jsonStartIndex > 0) {
                                contentStr = contentStr.substring(jsonStartIndex);
                            }

                            JSONObject contentJson = JSON.parseObject(contentStr);
                            int score = contentJson.getIntValue("sentiment_score");

                            // 关键过滤逻辑：只保留极度悲观(<=2)或极度乐观(>=8)
                            if (score > 2 && score < 8) {
                                resultFuture.complete(Collections.emptyList());
                                return;
                            }

                            // 5. 补充数据回传
                            node.put("sentiment_score", score);
                            node.put("sentiment_reason", contentJson.getString("sentiment_reason"));
                            node.put("raw_content", summary);
                            node.put("sentiment_es", Instant.now().atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli());

                            resultFuture.complete(Collections.singletonList(node.toJSONString()));
                        } catch (Exception e) {
                            LOG.error("[{}] Sentiment analysis parsing failed: {}, raw response might be malformed.", traceId, e.getMessage());
                            resultFuture.complete(Collections.emptyList());
                        }
                    }

                    @Override
                    public void failed(Exception ex) {
                        LOG.error("[{}] HTTP Request failed: {}", traceId, ex.getMessage());
                        resultFuture.complete(Collections.emptyList());
                    }

                    @Override
                    public void cancelled() {
                        LOG.warn("[{}] HTTP Request cancelled", traceId);
                        resultFuture.complete(Collections.emptyList());
                    }
                });
            } catch (Exception e) {
                LOG.error("[{}] Async execute exception: {}", traceId, e.getMessage());
                resultFuture.complete(Collections.emptyList());
            }
        });
    }

    @Override
    public void close() throws Exception {
        if (httpClient != null) {
            httpClient.close();
        }
    }
}
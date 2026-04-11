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

public class EthSentimentOllamaFunction extends RichAsyncFunction<String, String> {
    private transient HttpClient client;
    private String ollamaUrl;

    @Override
    public void open(Configuration parameters) {
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        MyParameter myParameter = new MyParameter(params);
        // 复用你之前的 host 获取逻辑
        this.ollamaUrl = "http://" + myParameter.getOllamaHost() + ":11434/api/generate";

        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .executor(Executors.newFixedThreadPool(10)) // 降低并发以保证 Qwen3:30b 的响应质量
                .build();
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) {
        // 构建面向 ETH 交易的情绪分析 Prompt
        String prompt = String.format(
                "你是一个资深 ETH 交易员。请分析以下内容的市场情绪：\n" +
                        "内容：'%s'\n" +
                        "要求：必须以 JSON 格式返回，包含字段 'score' (1-10分，1最悲观，10最看涨) 和 'reason' (简短理由)。",
                input);

        Map<String, Object> body = new HashMap<>();
        body.put("model", "qwen3-coder:30b");
        body.put("prompt", prompt);
        body.put("stream", false); // 必须关闭 stream 模式以获取完整响应

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(ollamaUrl))
                .POST(HttpRequest.BodyPublishers.ofString(JSON.toJSONString(body)))
                .build();

        client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .whenComplete((resp, throwable) -> {
                    try {
                        if (resp != null && resp.statusCode() == 200) {
                            JSONObject ollamaResp = JSON.parseObject(resp.body());
                            String aiResponse = ollamaResp.getString("response");

                            // 封装并清洗结果
                            JSONObject finalResult = new JSONObject();
                            finalResult.put("raw_content", input);
                            finalResult.put("ai_analysis", JSON.parseObject(aiResponse));
                            finalResult.put("timestamp", System.currentTimeMillis());

                            resultFuture.complete(Collections.singletonList(finalResult.toJSONString()));
                        } else {
                            resultFuture.complete(Collections.emptyList());
                        }
                    } catch (Exception e) {
                        resultFuture.complete(Collections.emptyList());
                    }
                });
    }
}
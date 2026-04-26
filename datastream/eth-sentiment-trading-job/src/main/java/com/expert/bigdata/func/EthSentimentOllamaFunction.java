package com.expert.bigdata.func;

import com.alibaba.fastjson.JSON;
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
                // 30s 容易因为本地 31b 大模型生成长 JSON 推理时超时导致 Flink Job 重启，放宽至 120s
                .setSocketTimeout(120000)
                .build();
        httpClient = HttpAsyncClients.custom().setDefaultRequestConfig(requestConfig).build();
        httpClient.start();
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
        JSONObject node = JSON.parseObject(input);
        String title = node.getString("title");
        String summary = node.getString("summary");
        String traceId = node.containsKey("id") ? node.getString("id") : "unknown";

        // 当 title 为空时，直接过滤掉这条数据
        if (title == null || title.trim().isEmpty()) {
            LOG.warn("[{}] Title is empty, filtering out this record.", traceId);
            resultFuture.complete(Collections.emptyList());
            return; // 结束当前处理，不发送给下游
        }

        // 当 summary 为空时，使用 title 的值填充 summary
        if (summary == null || summary.trim().isEmpty()) {
            summary = title;
            // 如果你需要将补全后的 summary 也一并传递给下游的 Flink 算子，可以将它写回 node
            node.put("summary", summary);
            LOG.info("[{}] Summary is empty, filled with Title.", traceId);
        }

        // 构建发给大模型分析的文本（因为上面已经保证了 title 和 summary 都有值，所以这里可以直接拼接）
        String contentToAnalyze = "Title: " + title + "\nSummary: " + summary;

        JSONObject requestBody = new JSONObject();
        // 匹配你本地的大模型
        requestBody.put("model", "deepseek-r1:8b");
        requestBody.put("stream", false);

        // 1. Prompt 升级：明确指定目标 JSON 格式，并优先输出核心结论（Score和Reason），把长推理链推理放到最后
        String prompt = "You are an expert ETH quantitative analyst...\n" +
                "{\n" +
                "  \"sentiment_score\": 5,\n" +
                "  \"sentiment_reason\": \"concise summary\",\n" +
                "  \"reasoning\": \"brief logic in 2 sentences\"\n" + // 明确要求简短
                "}\n\n" +
                "Text: " + contentToAnalyze;
        requestBody.put("prompt", prompt);

        // 2. 参数重置：恢复 repeat_penalty 并彻底关闭底层 format 掩码。
        // 不加 repeat_penalty 会导致部分量化模型原生陷入死循环 (same same same)。
        // 为了避免 repeat_penalty 与 JSON 掩码碰撞，我们直接不设 format 参数，完全依靠我们的截取逻辑。
        JSONObject options = new JSONObject();
        options.put("temperature", 0.6); // 适当增高温度保障流畅度
        options.put("top_p", 0.9);
        options.put("top_k", 40);
        options.put("repeat_penalty", 1.15); // 轻度重复惩罚防止原生复读机死循环
        options.put("num_predict", 3072); // 给足够的推理空间输出 JSON

        requestBody.put("options", options);

        // 3. 架构控制：完全依赖纯文本抽取 (indexOf { ... })，不使用底层强制 json 生成
        // 避免底层的 Logits 操控干扰 Gemma 模型特性

        HttpPost request = new HttpPost(apiUrl);
        request.setHeader("Content-Type", "application/json");
        request.setEntity(new StringEntity(requestBody.toJSONString(), "UTF-8"));

        String finalSummary = summary;
        CompletableFuture.runAsync(() -> {
            try {
                httpClient.execute(request, new org.apache.http.concurrent.FutureCallback<HttpResponse>() {
                    @Override
                    public void completed(HttpResponse response) {
                        try {
                            // 1. 强制指定 UTF-8 解码，防止多字节字符被截断产生乱码
                            String resJson = EntityUtils.toString(response.getEntity(), "UTF-8");
                            JSONObject resNode = JSON.parseObject(resJson);
                            String contentStr = resNode.getString("response");

                            // 如果 contentStr 是空的，直接退出
                            if (contentStr == null || contentStr.trim().isEmpty()) {
                                LOG.warn("[{}] Model returned empty response.", traceId);
                                resultFuture.complete(Collections.emptyList());
                                return;
                            }

                            // 2. 终极防御：掐头去尾提取纯净 JSON
                            // 寻找第一个 '{' 和最后一个 '}'
                            int startIdx = contentStr.indexOf("{");
                            int endIdx = contentStr.lastIndexOf("}");

                            if (startIdx >= 0 && endIdx >= startIdx) {
                                // 严格截取出有效的 JSON 对象部分，丢弃两端可能存在的 ```json 或  等特殊字符
                                contentStr = contentStr.substring(startIdx, endIdx + 1);
                            } else {
                                // 如果连花括号都没有，说明模型完全胡言乱语了
                                LOG.error("[{}] Model did not return a JSON object. Raw: {}", traceId, contentStr);
                                resultFuture.complete(Collections.emptyList());
                                return;
                            }

                            // 3. 开始解析纯净的 JSON
                            JSONObject contentJson = JSON.parseObject(contentStr);
                            int score = contentJson.getIntValue("sentiment_score");

                            // 关键过滤逻辑：只保留极度悲观(<=2)或极度乐观(>=8)
//                            if (score > 2 && score < 8) {
//                                resultFuture.complete(Collections.emptyList());
//                                return;
//                            }

                            // 补充数据回传
                            node.put("sentiment_score", score);
                            node.put("sentiment_reason", contentJson.getString("sentiment_reason"));
                            node.put("raw_content", finalSummary);
                            node.put("sentiment_es", Instant.now().atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli());

                            resultFuture.complete(Collections.singletonList(node.toJSONString()));
                        } catch (Exception e) {
                            // 4. 留下犯罪现场：如果还是解析失败，把模型原始的返回打印出来！
                            // 这一步对于架构排查至关重要
                            try {
                                String rawFailedText = EntityUtils.toString(response.getEntity(), "UTF-8");
                                LOG.error("[{}] Sentiment parsing failed. Error: {}, RAW Response: {}", traceId, e.getMessage(), rawFailedText);
                            } catch (Exception ex) {
                                LOG.error("[{}] Sentiment parsing failed: {}", traceId, e.getMessage());
                            }
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
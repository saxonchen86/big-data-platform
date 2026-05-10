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
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * 利用 NIO 异步回调实现的 Embedding 节点
 */
public class EthEmbeddingFunction extends RichAsyncFunction<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(EthEmbeddingFunction.class);

    private transient CloseableHttpAsyncClient httpClient;
    private String embeddingUrl;

    @Override
    public void open(Configuration parameters) {
        // 1. 从全局配置获取 URL
        embeddingUrl = getRuntimeContext().getExecutionConfig().getGlobalJobParameters()
                .toMap().getOrDefault("ollama.embedding.url", "http://localhost:11434/api/embeddings");

        // 2. 初始化异步 HTTP 客户端
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(5000)
                .setSocketTimeout(10000) // 考虑到 Embedding 可能耗时，建议设置稍长
                .build();

        httpClient = HttpAsyncClients.custom()
                .setDefaultRequestConfig(requestConfig)
                .setMaxConnTotal(100)    // 增加连接池上限
                .setMaxConnPerRoute(50)
                .build();

        httpClient.start();
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) {
        try {
            // Fastjson 解析输入
            JSONObject node = JSON.parseObject(input);
            String content = node.getString("raw_content");

            // 构造请求体
            JSONObject body = new JSONObject();
            body.put("model", "nomic-embed-text");
            body.put("prompt", content);

            HttpPost post = new HttpPost(embeddingUrl);
            post.setEntity(new StringEntity(body.toJSONString(), ContentType.APPLICATION_JSON));

            // 3. 真正执行 NIO 异步请求
            httpClient.execute(post, new FutureCallback<HttpResponse>() {
                @Override
                public void completed(HttpResponse response) {
                    try {
                        String responseStr = EntityUtils.toString(response.getEntity(), "UTF-8");
                        JSONObject res = JSON.parseObject(responseStr);

                        // 提取向量并塞回原始节点
                        JSONArray vector = res.getJSONArray("embedding");
                        if (vector != null) {
                            node.put("vector", vector);
                            resultFuture.complete(Collections.singletonList(node.toJSONString()));
                        } else {
                            LOG.warn("Ollama 返回的向量为空: {}", responseStr);
                            resultFuture.complete(Collections.emptyList());
                        }
                    } catch (Exception e) {
                        LOG.error("解析 Embedding 响应异常", e);
                        resultFuture.complete(Collections.emptyList());
                    }
                }

                @Override
                public void failed(Exception ex) {
                    LOG.error("Ollama Embedding 请求失败: {}", ex.getMessage());
                    resultFuture.complete(Collections.emptyList());
                }

                @Override
                public void cancelled() {
                    LOG.warn("Ollama Embedding 请求被取消");
                    resultFuture.complete(Collections.emptyList());
                }
            });

        } catch (Exception e) {
            LOG.error("asyncInvoke 内部异常, input: {}", input, e);
            resultFuture.complete(Collections.emptyList());
        }
    }

    @Override
    public void close() throws Exception {
        if (httpClient != null) {
            httpClient.close();
        }
    }
}
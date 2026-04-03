//package com.expert.bigdata;
//
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.functions.async.ResultFuture;
//import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
//import java.util.Collections;
//import java.util.concurrent.CompletableFuture;
//
//public class AsyncEmbeddingFunction extends RichAsyncFunction<String, VectorDocument> {
//
//    private transient AsyncHttpClient client;
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        // 初始化异步 HTTP 客户端
//        client = new AsyncHttpClient();
//    }
//
//    @Override
//    public void asyncInvoke(String rawText, ResultFuture<VectorDocument> resultFuture) {
//        // 发送非阻塞请求给 Python Embedding 服务
//        CompletableFuture<float[]> future = client.getEmbedding(rawText);
//
//        future.whenComplete((vector, error) -> {
//            if (vector != null) {
//                // 拼接原始文本和向量，发送给下游
//                VectorDocument doc = new VectorDocument(rawText, vector);
//                resultFuture.complete(Collections.singletonList(doc));
//            } else {
//                // 处理异常重试或丢弃到死信队列 (DLQ)
//                resultFuture.completeExceptionally(error);
//            }
//        });
//    }
//}
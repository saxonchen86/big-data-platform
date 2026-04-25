package com.expert.bigdata.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.milvus.client.MilvusServiceClient;
import io.milvus.param.ConnectParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.response.SearchResultsWrapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 基于 Milvus 的特征相似度历史回测与交易决策节点
 */
public class EthBacktestDecisionFunction extends RichAsyncFunction<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(EthBacktestDecisionFunction.class);
    private transient MilvusServiceClient milvusClient;

    @Override
    public void open(Configuration parameters) {
        var params = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();
        milvusClient = new MilvusServiceClient(ConnectParam.newBuilder()
                .withHost(params.get("milvus.host"))
                .withPort(Integer.parseInt(params.get("milvus.port")))
                .build());
    }

    // 修复 4: 增加 close 方法，防止 Milvus 连接泄漏
    @Override
    public void close() throws Exception {
        if (milvusClient != null) {
            milvusClient.close();
        }
        super.close();
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) {
        // 修复 1: 必须使用 CompletableFuture 包装同步的 Milvus 查询，否则 Async I/O 无效
        CompletableFuture.runAsync(() -> {
            try {
                JSONObject node = JSON.parseObject(input);

                // 修复 2: 严格类型转换，强制将 JSON 数组解析为 List<Float>
                JSONArray vectorArray = node.getJSONArray("vector");
                List<Float> vector = new ArrayList<>();
                if (vectorArray != null) {
                    for (int i = 0; i < vectorArray.size(); i++) {
                        vector.add(vectorArray.getFloat(i));
                    }
                }

                // 构造标量过滤 (注意: 此处提取 sentiment_score 使用 getLong 规避之前的类型问题)
                String expr = String.format("sentiment_score == %d && rsi_14 >= %.2f && is_settled == true",
                        node.getLong("sentiment_score"),
                        node.getDouble("rsi_14") - 5);

                SearchParam searchParam = SearchParam.newBuilder()
                        .withCollectionName("eth_sentiment_analysis")
                        // 修复 3: 将度量类型修改为 COSINE，以匹配你下方 score > 0.9 的业务逻辑
                        .withMetricType(io.milvus.param.MetricType.COSINE)
                        .withOutFields(java.util.Arrays.asList("win_rate", "return"))
                        .withTopK(5)
                        .withVectors(Collections.singletonList(vector))
                        .withVectorFieldName("vector")
                        .withExpr(expr)
                        .build();

                io.milvus.param.R<io.milvus.grpc.SearchResults> searchResp = milvusClient.search(searchParam);

                double winCount = 0;
                int validMatches = 0;
                double maxSimilarity = 0;

                if (searchResp.getStatus() == io.milvus.param.R.Status.Success.getCode() && searchResp.getData() != null) {
                    SearchResultsWrapper wrapper = new SearchResultsWrapper(searchResp.getData().getResults());
                    List<SearchResultsWrapper.IDScore> scores = wrapper.getIDScore(0);

                    for (SearchResultsWrapper.IDScore res : scores) {
                        // 由于使用的是 COSINE，分数在 [-1, 1] 之间，越大越相似
                        if (res.getScore() > 0.9) {
                            validMatches++;
                            maxSimilarity = Math.max(maxSimilarity, res.getScore());

                            // 解析回测收益
                            Object histReturnObj = res.get("return");
                            float histReturn = 0;
                            if (histReturnObj instanceof Number) {
                                histReturn = ((Number) histReturnObj).floatValue();
                            }
                            if (histReturn > 0) {
                                winCount++;
                            }
                        }
                    }
                }

                double avgWinRate = (validMatches > 0) ? (winCount / validMatches) : 0;

                // 目标决策：最大相似度 > 90% 且 胜率 > 65%
//                if (maxSimilarity > 0.9 && avgWinRate > 0.65) {
                if (maxSimilarity >= 0.0) { // 用于测试，获取原始数据
                    JSONObject signal = new JSONObject();
                    if (node.getLong("sentiment_score") > 8) {
                        signal.put("action", "BUY");
                    } else if (node.getLong("sentiment_score") < 2) {
                        signal.put("action", "SELL");
                    } else {
                        signal.put("action", "HOLD");
                    }
                    signal.put("token", "ETH");
                    signal.put("pubDate", node.getLong("pubDate"));
                    signal.put("sentiment_es", node.getLong("sentiment_es"));
                    signal.put("buy_time", Instant.now().atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli());
                    signal.put("sell_time", null);
                    signal.put("trigger_score", node.getLong("sentiment_score"));
                    signal.put("timestamp", Instant.now().atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli());

                    LOG.info("🔥 [交易指令] 匹配到相似历史！最大相似度: {}, 胜率: {}", maxSimilarity, avgWinRate);
                    resultFuture.complete(Collections.singletonList(signal.toJSONString()));
                } else {
                    resultFuture.complete(Collections.emptyList());
                }
            } catch (Exception e) {
                LOG.error("Milvus 回测决策节点发生异常: ", e);
                resultFuture.complete(Collections.emptyList());
            }
        });
    }
}
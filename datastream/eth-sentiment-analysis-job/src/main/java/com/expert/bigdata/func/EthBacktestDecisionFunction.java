package com.expert.bigdata.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.common.utils.MyParameter;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.response.SearchResp;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.*;

public class EthBacktestDecisionFunction extends RichAsyncFunction<String, String> {
    private transient MilvusClientV2 client;

    @Override
    public void open(Configuration parameters) {
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        MyParameter myParameter = new MyParameter(params);
        String host = myParameter.getMilvusHost();
        this.client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://" + host + ":19530").build());
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) {
        JSONObject node = JSON.parseObject(input);
        List<Float> currentVector = node.getJSONArray("vector").toJavaList(Float.class);
        int sentimentScore = node.getInteger("score");

        // 只有强力情绪才触发回测（例如 score > 8 或 score < 3）
        if (sentimentScore < 8 && sentimentScore > 3) {
            resultFuture.complete(Collections.singletonList(input));
            return;
        }

        // 1. 去 Milvus 搜索历史上相似度高的记录
        // 修复后的代码：直接传入 List<Float> 即可
        SearchReq searchReq = SearchReq.builder()
                .collectionName("eth_sentiment_analysis")
                .data(Collections.singletonList(currentVector)) // 直接传 List<Float>，不需要包装类
                .filter("is_settled == true")
                .topK(10)
                .outputFields(Arrays.asList("return_24h"))
                .build();

        SearchResp searchResp = client.search(searchReq);
        List<List<SearchResp.SearchResult>> results = searchResp.getSearchResults();

        double winCount = 0;
        int validMatches = 0;
        double maxSimilarity = 0;

        if (!results.isEmpty()) {
            for (SearchResp.SearchResult res : results.get(0)) {
                // Milvus COSINE 相似度 0.9 = 90%
                res.getEntity();
//                if (res.getScore() > 0.9) {
//                    validMatches++;
//                    maxSimilarity = Math.max(maxSimilarity, res.getScore());
//                    // 收益大于0即为“赢”
//                    float histReturn = (float) res.getEntity().get("return_24h");
//                    if (histReturn > 0) winCount++;
//                }
            }
        }

        double winRate = (validMatches > 0) ? (winCount / validMatches) : 0;

        // 2. 目标决策：相似度 > 90% 且 胜率 > 65%
        if (maxSimilarity > 0.9 && winRate > 0.65) {
            node.put("decision", "EXECUTE_BUY");
            node.put("win_rate_calc", winRate);
            System.out.println("🔥 [交易指令] 匹配到相似历史！胜率: " + winRate);
        } else {
            node.put("decision", "HOLD");
        }

        resultFuture.complete(Collections.singletonList(node.toJSONString()));
    }
}
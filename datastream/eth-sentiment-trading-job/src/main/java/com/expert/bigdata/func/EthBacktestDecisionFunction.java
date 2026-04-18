package com.expert.bigdata.func;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.milvus.client.MilvusServiceClient;
import io.milvus.param.ConnectParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.response.SearchResultsWrapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;

public class EthBacktestDecisionFunction extends RichAsyncFunction<String, String> {
    private final ObjectMapper mapper = new ObjectMapper();
    private transient MilvusServiceClient milvusClient;

    @Override
    public void open(Configuration parameters) {
        var params = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();
        milvusClient = new MilvusServiceClient(ConnectParam.newBuilder()
                .withHost(params.get("milvus.host"))
                .withPort(Integer.parseInt(params.get("milvus.port")))
                .build());
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) {
        try {
            ObjectNode node = (ObjectNode) mapper.readTree(input);
            List<Float> vector = mapper.convertValue(node.get("vector"), List.class);

            // 构造标量过滤
            String expr = String.format("sentiment_score == %d && rsi_14 >= %.2f && is_settled == true",
                    node.get("sentiment_score").asInt(), node.get("rsi_14").asDouble() - 5);

            SearchParam searchParam = SearchParam.newBuilder()
                    .withCollectionName("eth_sentiment_analysis")
                    .withMetricType(io.milvus.param.MetricType.L2)
                    .withOutFields(Collections.singletonList("win_rate"))
                    .withTopK(5)
                    .withVectors(Collections.singletonList(vector))
                    .withVectorFieldName("vector")
                    .withExpr(expr)
                    .build();

            SearchResultsWrapper results = milvusClient.search(searchParam).getData();
            // 简单胜率决策
            double avgWinRate = 0.7; // 实际应从 results 获取

            if (avgWinRate >= 0.65) {
                ObjectNode signal = mapper.createObjectNode();
                signal.put("action", "BUY");
                signal.put("token", "ETH");
                signal.put("pubDate", node.get("pubDate").asLong());
                signal.put("sentiment_es", node.get("sentiment_es").asLong());
                signal.put("buy_time", Instant.now().atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli());
                signal.set("sell_time", null);
                signal.put("trigger_score", node.get("sentiment_score").asInt());
                signal.put("timestamp", Instant.now().atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli());

                resultFuture.complete(Collections.singletonList(mapper.writeValueAsString(signal)));
            } else {
                resultFuture.complete(Collections.emptyList());
            }
        } catch (Exception e) {
            resultFuture.complete(Collections.emptyList());
        }
    }
}
package com.expert.bigdata.func;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.milvus.client.MilvusServiceClient;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.dml.InsertParam;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * 记忆存入 Sink：将实时特征与向量存入 Milvus
 * 遵循：is_settled 默认为 false，traceId 追踪
 */
public class MilvusSink extends RichSinkFunction<String> {
    private static final Logger LOG = LoggerFactory.getLogger(MilvusSink.class);
    private final ObjectMapper mapper = new ObjectMapper();
    private transient MilvusServiceClient milvusClient;
    private String collectionName;

    @Override
    public void open(Configuration parameters) throws Exception {
        var params = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();

        // 1. 初始化 Milvus 连接
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost(params.get("milvus.host"))
                .withPort(Integer.parseInt(params.get("milvus.port")))
                .build();
        milvusClient = new MilvusServiceClient(connectParam);

        collectionName = "eth_sentiment_analysis";
        LOG.info("MilvusSink initialized for collection: {}", collectionName);
    }

    @Override
    public void invoke(String value, Context context) {
        String traceId = "unknown";
        try {
            JsonNode node = mapper.readTree(value);
            traceId = node.get("id").asText();

            // 1. 提取向量 (ArrayNode -> List<Float>)
            List<Float> vector = StreamSupport.stream(node.get("vector").spliterator(), false)
                    .map(v -> (float) v.asDouble())
                    .collect(Collectors.toList());

            // 2. 准备数据行 (Fields 必须与 Milvus Schema 一致)
            List<InsertParam.Field> fields = new ArrayList<>();
            // 主键或唯一标识
            fields.add(new InsertParam.Field("event_id", Collections.singletonList(node.get("id").asText())));
            fields.add(new InsertParam.Field("title", Collections.singletonList(node.get("title").asText())));
            fields.add(new InsertParam.Field("sentiment_score", Collections.singletonList(node.get("sentiment_score").asInt())));
            fields.add(new InsertParam.Field("rsi_14", Collections.singletonList(node.get("rsi_14").asDouble())));
            fields.add(new InsertParam.Field("atr_14", Collections.singletonList(node.get("atr_14").asDouble())));
            fields.add(new InsertParam.Field("price_at_t", Collections.singletonList(node.get("price_at_t").asDouble())));
            fields.add(new InsertParam.Field("pub_date", Collections.singletonList(node.get("pubDate").asLong())));
            fields.add(new InsertParam.Field("sentiment_es", Collections.singletonList(node.get("sentiment_es").asLong())));

            // 向量字段 (必须是 List<List<Float>>)
            List<List<Float>> vectorList = new ArrayList<>();
            vectorList.add(vector);
            fields.add(new InsertParam.Field("vector", vectorList));

            // 初始状态：未结算
            fields.add(new InsertParam.Field("is_settled", Collections.singletonList(false)));
            // 预留胜率字段，初始为 0
            fields.add(new InsertParam.Field("win_rate", Collections.singletonList(0.0)));

            // 3. 执行插入
            InsertParam insertParam = InsertParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withFields(fields)
                    .build();

            R<io.milvus.grpc.MutationResult> response = milvusClient.insert(insertParam);

            if (response.getStatus() != R.Status.Success.getCode()) {
                LOG.error("[{}] Milvus insert failed: {}", traceId, response.getMessage());
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[{}] Successfully indexed to Milvus memory.", traceId);
                }
            }

        } catch (Exception e) {
            LOG.error("[{}] Critical error in MilvusSink: {}", traceId, e.getMessage(), e);
        }
    }

    @Override
    public void close() throws Exception {
        if (milvusClient != null) {
            milvusClient.close();
        }
    }
}
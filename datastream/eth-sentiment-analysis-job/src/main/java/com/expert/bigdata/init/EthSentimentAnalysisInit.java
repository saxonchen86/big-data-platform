package com.expert.bigdata.init;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;

import java.util.ArrayList;
import java.util.List;

public class EthSentimentAnalysisInit {
    public static void main(String[] args) {
        // 1. 连接 Milvus (复用你之前的配置)
        MilvusClientV2 client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://milvus-standalone:19530") // 生产环境请根据 MyParameter 传入
                .build());

        // 2. 定义 Schema
        CreateCollectionReq.CollectionSchema schema = client.createSchema();

        // 主键 ID (自动递增)
        schema.addField(AddFieldReq.builder().fieldName("id").dataType(DataType.Int64).isPrimaryKey(true).autoID(true).build());
        // 原始文本 (长文本建议给够长度，如 5000)
        schema.addField(AddFieldReq.builder().fieldName("raw_content").dataType(DataType.VarChar).maxLength(5000).build());
        // 情绪分数标量
        schema.addField(AddFieldReq.builder().fieldName("sentiment_score").dataType(DataType.Int32).build());
        // 情绪分析理由
        schema.addField(AddFieldReq.builder().fieldName("sentiment_reason").dataType(DataType.VarChar).maxLength(1000).build());
        // 时间戳
        schema.addField(AddFieldReq.builder().fieldName("timestamp").dataType(DataType.Int64).build());
        // 向量字段 (nomic-embed-text 维度通常是 768)
        schema.addField(AddFieldReq.builder().fieldName("vector").dataType(DataType.FloatVector).dimension(768).build());

        // 3. 设置索引参数 (为了能够进行向量搜索)
        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName("vector")
                .indexType(IndexParam.IndexType.IVF_FLAT)
                .metricType(IndexParam.MetricType.COSINE) // 推荐使用余弦相似度进行情绪文本匹配
                .extraParams(java.util.Collections.singletonMap("nlist", "128"))
                .build());

        // 4. 正式创建集合
        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                .collectionName("eth_sentiment_analysis")
                .collectionSchema(schema)
                .indexParams(indexParams)
                .build();

        client.createCollection(createCollectionReq);

        System.out.println("✅ Milvus 集合 'eth_sentiment_analysis' 创建成功！");
    }
}
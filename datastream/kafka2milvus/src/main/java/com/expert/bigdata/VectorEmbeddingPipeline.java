//package com.expert.bigdata;
//
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
//import org.apache.flink.streaming.api.datastream.AsyncDataStream;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import java.util.concurrent.TimeUnit;
//
//public class VectorEmbeddingPipeline {
//
//    public static void main(String[] args) throws Exception {
//        // 1. 初始化 Flink 运行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(5000); // 开启精确一次语义，避免向量重复写入
//
//        // 2. 接入 Kafka 3.7.0 数据源
//        DataStream<String> rawTextStream = env.fromSource(
//                KafkaSourceBuilder.buildSource(),
//                WatermarkStrategy.noWatermarks(),
//                "Kafka-Source"
//        );
//
//        // 3. 核心亮点：异步调用 Embedding 模型 (非阻塞)
//        // 这里不要用普通的 map()，必须用 Async I/O
//        int capacity = 1000; // 允许同时 in-flight 的异步请求数量
//        DataStream<VectorDocument> embeddedStream = AsyncDataStream.unorderedWait(
//                rawTextStream,
//                new AsyncEmbeddingFunction(), // 你自定义的异步调用外部 Python API 的逻辑
//                3000, TimeUnit.MILLISECONDS, // 超时时间
//                capacity
//        );
//
//        // 4. 写入向量数据库 (Sink)
//        embeddedStream.addSink(new MilvusVectorSink())
//                .name("Milvus-Vector-Sink");
//
//        env.execute("Real-time AI Feature Pipeline");
//    }
//}
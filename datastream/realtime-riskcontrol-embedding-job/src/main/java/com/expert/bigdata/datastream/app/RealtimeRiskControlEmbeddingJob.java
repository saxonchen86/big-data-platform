package com.expert.bigdata.datastream.app;

import com.expert.bigdata.datastream.func.OllamaAsyncEmbeddingFunction;
import com.expert.bigdata.datastream.func.VectorDatabaseSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class RealtimeRiskControlEmbeddingJob {

    public static void main(String[] args) throws Exception {
        // 1. 初始化环境并解析自适应参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params); // 关键：全局共享参数
        env.enableCheckpointing(5000);

        // 2. Kafka Source (使用外部监听端口 9092)
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(params.get("kafkaUrl", "localhost:9092"))
                .setTopics(params.get("kafkaTopics", "risk_control_logs"))
                .setGroupId(params.get("kafkaGroupId", "dofi-group-v3"))
                .setStartingOffsets(OffsetsInitializer.latest()) // 确保能读到旧数据
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 3. 异步向量化算子
        DataStream<String> embeddedStream = AsyncDataStream.unorderedWait(
                kafkaStream,
                new OllamaAsyncEmbeddingFunction(),
                10000, TimeUnit.MILLISECONDS,
                100
        );

        // 4. 写入 Milvus Sink (必须确保 addSink 被正确调用)
        embeddedStream.addSink(new VectorDatabaseSink()).name("Milvus-Sink");

        // 5. 启动任务
        env.execute("Dofi-Realtime-AI-Pipeline");
    }
}
package com.expert.bigdata.app;

import com.expert.bigdata.func.OllamaAsyncEmbeddingFunction;
import com.expert.bigdata.func.VectorDatabaseSink;
import com.bigdata.common.utils.MyParameter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

// 本地运行
// --kafkaUrl localhost:9092  --sourceTopic risk_control_logs  --kafkaGroupId dofi-group-v3

// --kafkaUrl kafka:29092
// --sourceTopic risk_control_logs
// --kafkaGroupId dofi-group-v3
// --ollamaHost host.docker.internal
public class RealtimeRiskControlEmbeddingJob {

    public static void main(String[] args) throws Exception {
        // 1. 初始化环境并解析自适应参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params); // 关键：全局共享参数
        env.enableCheckpointing(5000);

        // 2. 使用 MyParameter 工具类解析 Kafka 连接参数
        MyParameter myParameter = new MyParameter(params);

        // 3. Kafka Source (使用外部监听端口 9092)
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(myParameter.getKafkaUrl())
                .setTopics(myParameter.getSourceTopic())
                .setGroupId(myParameter.getKafkaGroupId())
                .setStartingOffsets(OffsetsInitializer.latest()) // 确保能读到旧数据
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 4. 异步向量化算子
        DataStream<String> embeddedStream = AsyncDataStream.unorderedWait(
                kafkaStream,
                new OllamaAsyncEmbeddingFunction(),
                30000L,
                TimeUnit.MILLISECONDS,
                100
        );

        // 5. 写入 Milvus Sink (必须确保 addSink 被正确调用)
        embeddedStream.addSink(new VectorDatabaseSink()).name("Milvus-Sink");

        // 6. 启动任务
        env.execute("Dofi-Realtime-AI-Pipeline");
    }
}
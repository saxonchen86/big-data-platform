package com.expert.bigdata.app;

import com.expert.bigdata.func.EthEmbeddingFunction;
import com.expert.bigdata.func.EthSentimentOllamaFunction;
import com.expert.bigdata.func.EthSentimentVectorDatabaseSink;
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

/**
 * ETH 市场情绪分析任务 (T+1 策略基础架构)
 */
public class EthSentimentAnalysisJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);
        env.enableCheckpointing(10000); // 10秒一次检查点

        MyParameter myParameter = new MyParameter(params);

        // 1. Kafka Source: 接入 Neynar 或 CryptoPanic 的数据流
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(myParameter.getKafkaUrl())
                .setTopics("eth_social_stream") // 建议 topic 名
                .setGroupId("eth-sentiment-analysis-v1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "ETH-Social-Source");

        // 2. 异步情绪分析算子：调用 Qwen3-Coder:30b 进行语义理解
        DataStream<String> sentimentStream = AsyncDataStream.unorderedWait(
                kafkaStream,
                new EthSentimentOllamaFunction(), // 自定义情绪分析函数
                60000L, // LLM 推理较慢，超时设为 60s
                TimeUnit.MILLISECONDS,
                // qwen3-coder:30b 运行时会占用大量显存，如果 Flink 任务出现 Timeout，unorderedWait 中调低并行度（如从 20 降到 10）
                20 // 控制并发数，保护 MacBook M4 Pro 的显存
        );

        // 2. 向量化算子 (nomic-embed-text)
        DataStream<String> embeddedStream = AsyncDataStream.unorderedWait(
                sentimentStream,
                new EthEmbeddingFunction(),
                30000L, TimeUnit.MILLISECONDS, 50
        );

        // 3. 写入 Milvus
        embeddedStream.addSink(new EthSentimentVectorDatabaseSink()).name("Milvus-ETH-Sentiment-Sink");

        // 3. 写入 Milvus Sink: 存储文本、分数及向量
        sentimentStream.addSink(new EthSentimentVectorDatabaseSink()).name("Milvus-Sentiment-Sink");

        env.execute("ETH-Market-Sentiment-Analysis");
    }
}
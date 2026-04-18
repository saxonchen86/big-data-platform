package com.expert.bigdata.app;

import com.expert.bigdata.func.*;
import com.expert.bigdata.func.EthSentimentVectorDatabaseSink;
import com.bigdata.common.utils.MyParameter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

// 本地idea运行参数
// --ollamaHost localhost

// streampark运行参数
// --ollamaHost host.docker.internal
// --milvusHost milvus-standalone

/**
 * ETH 市场情绪分析任务 (T+1 策略基础架构)
 */
public class EthSentimentAnalysisJob {

    // 使用连接池管理 HTTP 客户端
    private static final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .executor(Executors.newFixedThreadPool(10))
                .build();

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
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
                // 找不到提交位点时，从最早的消息开始读（推荐用于排查数据丢失问题）
//                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setProperty("partition.discovery.interval.ms", "10000")
                // 使用 Netty 或原生 NIO 优化网络传输
                // 在 Kafka 消费者配置中启用零拷贝
                .setProperty("enable.auto.commit", "false")
                .setProperty("auto.offset.reset", "latest")
                .setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .setProperty("fetch.min.bytes", "1024")
                .setProperty("fetch.max.wait.ms", "500")
                .build();

        DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "ETH-Social-Source");

        // 数据清洗算子：过滤掉
//        kafkaStream.print(); // 打印数据流

        // 2. 异步情绪分析算子：调用 大模型 进行语义理解
        DataStream<String> sentimentStream = AsyncDataStream.unorderedWait(
                kafkaStream,
                new EthSentimentOllamaFunction(), // 自定义情绪分析函数
                60000L, // LLM 推理较慢，超时设为 60s
                TimeUnit.MILLISECONDS,
                // 模型运行时会占用大量显存，如果 Flink 任务出现 Timeout，unorderedWait 中调低并行度（如从 20 降到 10）
                20 // 控制并发数，保护 MacBook M4 Pro 的显存
        );

        //-- 判断情绪大于 8 再进行向量化 向量化算子 (nomic-embed-text)
        DataStream<String> embeddedStream = AsyncDataStream.unorderedWait(
                sentimentStream,
                new EthEmbeddingFunction(),
                30000L, TimeUnit.MILLISECONDS, 50
        );

        //-- 情绪向量化后，触发 EthBacktestDecisionFunction 判断是否买入
        SingleOutputStreamOperator<String> backtestDecisionStream = AsyncDataStream.unorderedWait(
                sentimentStream,
                new EthBacktestDecisionFunction(),
                30000L, TimeUnit.MILLISECONDS, 50
        );

        // 3. 写入 Milvus
        embeddedStream.addSink(new EthSentimentVectorDatabaseSink()).name("Milvus-ETH-Sentiment-Sink");

        env.execute("ETH-Market-Sentiment-Analysis");
    }
}

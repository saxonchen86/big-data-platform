package com.expert.bigdata.app;

//import com.expert.bigdata.app.model.EthereumData;;
import com.expert.bigdata.func.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.concurrent.TimeUnit;

public class EthSentimentTradingJob {
    public static void main(String[] args) throws Exception {
        // 1. 获取配置 (优先读取 application.properties)
        final ParameterTool params = ParameterTool.fromPropertiesFile(
                EthSentimentTradingJob.class.getClassLoader().getResourceAsStream("application.properties"));

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        // 2. Kafka 数据源
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(params.get("kafka.bootstrap.servers"))
                .setTopics(params.get("kafka.topic.social"))
                .setGroupId(params.get("kafka.group.id"))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "SocialStream");

        /*
            Undo：风控流处理
            特点：命中率高，过滤数据
         */

        // 3. 算子链装配

        // 阶段 1: 情感分析 (LLM llama.cpp)
        DataStream<String> sentimentStream = AsyncDataStream.orderedWait(
                rawStream, new EthSentimentOllamaFunction(), 30, TimeUnit.SECONDS, 100);

        // 阶段 2: 获取 MySQL 历史 K 线特征
        DataStream<String> featureStream = AsyncDataStream.orderedWait(
                sentimentStream, new EthPriceFeatureAsyncFunction(), 5, TimeUnit.SECONDS, 100);

        // 阶段 3: 生成向量 Embedding
        DataStream<String> embeddingStream = AsyncDataStream.orderedWait(
                featureStream, new EthEmbeddingFunction(), 10, TimeUnit.SECONDS, 50);

        // 阶段 4: Milvus 回测决策
        DataStream<String> decisionStream = AsyncDataStream.orderedWait(
                embeddingStream, new EthBacktestDecisionFunction(), 10, TimeUnit.SECONDS, 50);

        /*
            Undo：风控流处理：
            特点：交易前风控，也可以认为是策略
            例如：当市场情绪看好，但是主力资金在逃离，说明是在大户砸盘的征兆，触发卖出级别高于模型判断
                这个风险控制可以交给风控AI，也就是另外一个模型来做
         */

        // 4. 发送到 Kafka 交易信号 Topic
        decisionStream.print("Trading Signal"); // 控制台预览
        // 此处可添加 KafkaSink 发送 decisionStream 至 topic_trade_signals

        // 5. 记忆存入 Sink
        // embeddingStream.addSink(new MilvusSink());
        embeddingStream.addSink(new MilvusSink()).name("MilvusMemorySink");

        env.execute("ETH Real-time Sentiment Quant Engine");
    }
}
import os
import json
import logging
from kafka import KafkaConsumer
from pymilvus import connections, Collection
from dotenv import load_dotenv

# 日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("SettlementWorker")

load_dotenv()

class MilvusSettlementWorker:
    def __init__(self):
        # 1. 连接 Milvus
        host = os.getenv("MILVUS_HOST", "milvus-standalone")
        port = os.getenv("MILVUS_PORT", "19530")
        logger.info(f"Connecting to Milvus at {host}:{port}...")
        connections.connect("default", host=host, port=port)

        self.collection_name = "eth_sentiment_analysis"
        self.collection = Collection(self.collection_name)
        # 确保集合加载到内存中以便查询
        self.collection.load()

        # 定义需要查出的完整字段（必须包含 vector 才能执行 upsert 覆盖）
        self.output_fields = [
            "event_id", "title", "sentiment_score", "rsi_14", "atr_14",
            "price_at_t", "pub_date", "sentiment_es", "vector",
            "is_settled", "win_rate", "return"
        ]

        # 2. 初始化 Kafka 消费者
        kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        self.topic = "topic_trade_signals"

        logger.info(f"Connecting to Kafka: {kafka_bootstrap}, listening on topic: {self.topic}")
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=kafka_bootstrap,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='milvus_settlement_group',
            auto_offset_reset='latest'
        )

    def process_sell_signal(self, signal):
        """
        处理卖出信号并回填 Milvus
        假设 signal 中包含: 'event_id' (主键), 'buy_price', 'sell_price'
        (或者直接包含了 'return_rate')
        """
        # 1. 提取必要标识
        # 注意：这里强依赖你的 SELL 信号里带上当时买入时记录的 event_id
        event_id = signal.get("event_id")
        if not event_id:
            logger.warning(f"SELL signal missing 'event_id', cannot settle: {signal}")
            return

        # 2. 计算收益率 (假设信号里给了买卖价格，如果没有，请根据你的实际 JSON 结构调整)
        # 如果你的信号直接给出了收益率，可以直接取: return_rate = signal.get("return_rate")
        buy_price = signal.get("buy_price", 0.0)
        sell_price = signal.get("sell_price", 0.0)

        if buy_price <= 0:
            logger.warning(f"Invalid buy_price in signal for event_id {event_id}. Skipping.")
            return

        return_rate = (sell_price - buy_price) / buy_price

        # 胜负判定：收益率 > 0 记为胜 (1.0)，否则为负 (0.0)
        win_rate_val = 1.0 if return_rate > 0 else 0.0

        logger.info(f"Settling event [{event_id}]: Return = {return_rate:.4f}, Win = {win_rate_val}")

        # 3. 从 Milvus 查出旧数据
        expr = f'event_id == "{event_id}"'
        res = self.collection.query(expr=expr, output_fields=self.output_fields)

        if not res:
            logger.error(f"Event ID {event_id} not found in Milvus. Cannot settle.")
            return

        record = res[0]

        if record.get("is_settled"):
            logger.info(f"Event ID {event_id} is already settled. Skipping.")
            return

        # 4. 修改标量数据
        record["is_settled"] = True
        record["return"] = float(return_rate)
        record["win_rate"] = float(win_rate_val)

        # 5. 执行 Upsert (覆盖重写)
        try:
            # PyMilvus 支持直接传入 list of dicts 进行 upsert
            self.collection.upsert([record])
            logger.info(f"✅ Successfully updated Milvus memory for event [{event_id}]")
        except Exception as e:
            logger.error(f"Failed to upsert memory for event [{event_id}]: {e}")

    def run_forever(self):
        logger.info("Listening for trade signals...")
        try:
            for message in self.consumer:
                signal = message.value
                action = signal.get("action")

                # 只处理 SELL 动作进行结算
                if action == "SELL":
                    self.process_sell_signal(signal)
                else:
                    # BUY 或 OBSERVE 信号忽略，因为不需要在此节点处理
                    pass
        except KeyboardInterrupt:
            logger.info("Shutting down settlement worker...")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    worker = MilvusSettlementWorker()
    worker.run_forever()
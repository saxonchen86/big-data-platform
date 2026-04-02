import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# 配置 Kafka 地址 (宿主机访问用 localhost)
KAFKA_SERVER = 'localhost:9092'
TOPIC_NAME = 'user_clicks'

def get_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            # 增加超时设置，防止脚本卡死
            request_timeout_ms=5000
        )
        return producer
    except Exception as e:
        print(f"❌ 无法连接 Kafka: {e}")
        return None

def generate_data():
    behaviors = ['click', 'view', 'cart', 'buy']
    data = {
        "user_id": str(random.randint(1000, 9999)),
        "item_id": str(random.randint(50000, 99999)),
        "behavior": random.choice(behaviors),
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    return data

def on_success(record_metadata):
    print(f"✅ 成功发送到 Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")

def on_error(exception):
    print(f"❌ 发送失败: {exception}")

if __name__ == "__main__":
    producer = get_producer()
    if producer:
        print(f"🚀 开始向 Topic [{TOPIC_NAME}] 写入数据...")
        try:
            # 批量发送数据
            records = []
            for i in range(100):
                records.append(generate_data())

            # 一次性发送批次数据
            for record in records:
                # 使用异步发送提高性能
                future = producer.send(TOPIC_NAME, value=record)
                # 建议的优化配置
                producer = KafkaProducer(
                    bootstrap_servers=[KAFKA_SERVER],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    # 批量发送配置
                    batch_size=16384,           # 批次大小
                    linger_ms=5,                # 等待时间
                    # 重试配置
                    retries=3,                  # 重试次数
                    retry_backoff_ms=1000,      # 重试间隔
                    # 性能配置
                    request_timeout_ms=30000,   # 请求超时
                    max_request_size=1048576,   # 最大请求大小
                    # 异步发送
                    acks='all'                  # 确认机制
                )
                future.add_callback(on_success)
                future.add_errback(on_error)
        except KeyboardInterrupt:
            print("\n🛑 停止发送。")
        finally:
            producer.close()
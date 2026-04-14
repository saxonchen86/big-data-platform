import requests
import time
import json
from kafka import KafkaProducer

# 配置信息
API_KEY = "你的_CRYPTOPANIC_API_KEY" # 免费版在官网注册即可获取
KAFKA_BOOTSTRAP_SERVERS = ['kafka:29092'] # 对应你的 Kafka 3.7.0
KAFKA_TOPIC = 'eth_social_stream'
CHECK_INTERVAL = 60 # 免费版建议每分钟请求一次，避免被封 IP

def get_crypto_panic_news():
    url = f"https://cryptopanic.com/api/v1/posts/?auth_token={API_KEY}&currencies=ETH&kind=news&public=true"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.json().get('results', [])
        else:
            print(f"Error: {response.status_code}")
            return []
    except Exception as e:
        print(f"Connection failed: {e}")
        return []

def run_ingestion():
    # 初始化 Kafka 生产者
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    last_news_id = None
    print(f"🚀 ETH 情绪采集任务已启动，目标 Topic: {KAFKA_TOPIC}")

    while True:
        news_list = get_crypto_panic_news()

        new_count = 0
        for news in news_list:
            news_id = news.get('id')

            # 简单的增量同步逻辑：只发送比上次更新的消息
            if last_news_id and news_id <= last_news_id:
                break

            # 构造发送给 Flink 的标准结构
            payload = {
                "id": news_id,
                "title": news.get('title'),
                "domain": news.get('domain'),
                "url": news.get('url'),
                "published_at": news.get('published_at'),
                "source_type": "cryptopanic"
            }

            producer.send(KAFKA_TOPIC, value=payload)
            new_count += 1

        if news_list:
            last_news_id = news_list[0].get('id')

        print(f"[{time.strftime('%H:%M:%S')}] 已推送 {new_count} 条新消息至 Kafka")
        producer.flush()

        # 遵循 Free 版 API 限制
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    run_ingestion()
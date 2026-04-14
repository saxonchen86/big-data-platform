import requests
from bs4 import BeautifulSoup
import time
import json
from kafka import KafkaProducer
from prometheus_client import start_http_server, Counter, Gauge

# --- 1. Prometheus 指标定义 ---
# 统计总处理量
NEWS_TOTAL = Counter('eth_news_total', 'Total number of news collected', ['source'])
# 统计最近一次抓取的数量（用于观察波动）
BATCH_SIZE = Gauge('eth_news_batch_size', 'Number of news in the last fetch', ['source'])

# --- 2. 配置 ---
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'eth_social_stream'
# 监控的推特账号（通过 Nitter 镜像）
MONITOR_ACCOUNTS = ['vitalikbuterin', 'ethereum', 'Starknet']
NITTER_INSTANCE = "https://nitter.net" # 如果失效可以换成 nitter.cz 或其他实例

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def scrape_nitter(account):
    """抓取 Nitter 上的推文"""
    url = f"{NITTER_INSTANCE}/{account}"
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

    new_tweets = 0
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200: return 0

        soup = BeautifulSoup(resp.content, 'html.parser')
        # Nitter 的推文在 .timeline-item 类中
        items = soup.find_all('div', class_='timeline-item')

        for item in items:
            content = item.find('div', class_='tweet-content')
            if content:
                text = content.get_text().strip()
                payload = {
                    "title": f"Tweet from @{account}",
                    "raw_content": text,
                    "source_type": "twitter_nitter",
                    "author": account,
                    "timestamp": int(time.time() * 1000)
                }
                producer.send(KAFKA_TOPIC, value=payload)
                new_tweets += 1

        # 更新 Prometheus 指标
        NEWS_TOTAL.labels(source=f'twitter_{account}').inc(new_tweets)
        BATCH_SIZE.labels(source=f'twitter_{account}').set(new_tweets)
        return new_tweets
    except Exception as e:
        print(f"Scrape {account} failed: {e}")
        return 0

def run_collector():
    # 在 8000 端口启动 Prometheus 指标服务
    start_http_server(8000)
    print("📊 Prometheus metrics available at http://localhost:8000")

    while True:
        # 1. 抓取 Twitter 辅助信号
        for acc in MONITOR_ACCOUNTS:
            count = scrape_nitter(acc)
            print(f"[{time.strftime('%H:%M:%S')}] Twitter @{acc}: {count} news")
            time.sleep(2) # 避免请求过快

        # 2. 抓取 CryptoPanic (复用之前的逻辑)
        # 这里可以加入你之前的 CryptoPanic 代码，并同步更新 NEWS_TOTAL.labels(source='cryptopanic').inc()

        producer.flush()
        # 每 5 分钟跑一轮大循环
        time.sleep(300)

if __name__ == "__main__":
    run_collector()
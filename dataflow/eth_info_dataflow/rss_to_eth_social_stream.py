import asyncio
import aiohttp
import feedparser
import logging
import time
import hashlib
import json
import email.utils
from typing import List, Set, Dict
from datetime import datetime, timezone, timedelta
from aiokafka import AIOKafkaProducer
from prometheus_client import start_http_server, Counter
from bs4 import BeautifulSoup

# --- 1. 配置日志 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 定义东八区（上海时间）
SH_TZ = timezone(timedelta(hours=8))

# --- 2. Prometheus 监控指标 ---
NEWS_SENT = Counter('rss_news_sent_total', 'Total news sent to Kafka', ['topic'])

class RSSCollector:
    def __init__(self, urls: List[str], bootstrap_servers: str = 'kafka:29092', check_interval: int = 60):
        self.urls = urls
        self.bootstrap_servers = bootstrap_servers
        self.check_interval = check_interval
        self.seen_entries: Set[str] = set()
        self.session = None
        self.producer = None

    def _generate_id(self, entry: Dict) -> str:
        """根据链接和标题生成唯一 ID，用于去重"""
        content = entry.get('link', '') + entry.get('title', '')
        return hashlib.md5(content.encode('utf-8')).hexdigest()

    async def start(self):
        """启动异步采集引擎"""
        logger.info("🚀 启动异步采集引擎...")

        # 初始化 Kafka 异步生产者
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        await self.producer.start()
        logger.info(f"📡 Kafka 生产者已连接至: {self.bootstrap_servers}")

        try:
            async with aiohttp.ClientSession() as session:
                self.session = session
                while True:
                    await self.run_once()
                    logger.info(f"💤 等待 {self.check_interval} 秒后进行下一轮抓取...")
                    await asyncio.sleep(self.check_interval)
        finally:
            await self.producer.stop()

    async def fetch_and_parse(self, url: str) -> List[Dict]:
        """异步获取并解析单个 RSS 源"""
        try:
            async with self.session.get(url, timeout=15) as response:
                if response.status != 200:
                    logger.warning(f"无法访问源 {url}, 状态码: {response.status}")
                    return []

                xml_content = await response.text()
                feed = feedparser.parse(xml_content)

                new_items = []
                for entry in feed.entries:
                    entry_id = self._generate_id(entry)
                    if entry_id not in self.seen_entries:
                        # 1. 清洗 HTML 标签，仅保留 summary 纯文本
                        raw_summary = entry.get('summary', '')
                        clean_summary = BeautifulSoup(raw_summary, "html.parser").text if raw_summary else ""

                        # 2. 处理 pubDate，转换为东八区毫秒时间戳
                        pub_date_str = entry.get('published')
                        pub_ts_ms = 0
                        if pub_date_str:
                            try:
                                # 解析 RSS 标准时间字符串
                                dt = email.utils.parsedate_to_datetime(pub_date_str)
                                # 转换为上海时间并提取毫秒时间戳
                                pub_ts_ms = int(dt.astimezone(SH_TZ).timestamp() * 1000)
                            except Exception as te:
                                logger.error(f"时间解析错误: {te}")

                        # 3. 构造精简后的 Item
                        item = {
                            "id": entry_id,
                            "title": entry.get('title'),
                            "summary": clean_summary,
                            "pubDate": pub_ts_ms,  # 毫秒时间戳
                            "source": url,
                            "es": int(time.time() * 1000) # 采集时间戳（原 timestamp）
                        }
                        new_items.append(item)
                        self.seen_entries.add(entry_id)
                return new_items
        except Exception as e:
            logger.error(f"抓取失败 {url}: {e}")
            return []

    async def process_item(self, item: Dict):
        """发送至 Kafka"""
        topic = "eth_social_stream"
        try:
            await self.producer.send_and_wait(topic, item)
            NEWS_SENT.labels(topic=topic).inc()
            logger.info(f"✅ 已同步至 Kafka: {item['title'][:40]}...")
        except Exception as e:
            logger.error(f"❌ Kafka 发送失败: {e}")

    async def run_once(self):
        tasks = [self.fetch_and_parse(url) for url in self.urls]
        results = await asyncio.gather(*tasks)
        all_new_items = [item for sublist in results for item in sublist]
        for item in all_new_items:
            await self.process_item(item)

if __name__ == "__main__":
    start_http_server(8000)

    crypto_feeds = [
        "https://cointelegraph.com/rss",
        "https://www.coindesk.com/arc/outboundfeeds/rss/",
        "https://decrypt.co/feed"
    ]

    collector = RSSCollector(
        urls=crypto_feeds,
        bootstrap_servers='kafka:29092',
        check_interval=60
    )

    try:
        asyncio.run(collector.start())
    except KeyboardInterrupt:
        logger.info("用户停止采集任务")
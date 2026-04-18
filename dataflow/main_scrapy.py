import asyncio
import json
import time
import random
from pydantic import BaseModel
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

# （保留你 main.py 原有的 SentimentData 和 TextCleaner 定义）

# ==========================================
# 3. 数据采集引擎 (整合 Playwright 架构)
# ==========================================
class CryptoPanicIngestor:
    def __init__(self, name: str, producer, cleaner):
        self.name = name
        self.producer = producer
        self.cleaner = cleaner
        # 你的 Kafka Topic 名称，请根据实际情况调整
        self.topic = "raw-crypto-news"

    async def start_polling(self):
        """主控调度器：负责定期拉起采集任务并控制频率"""
        print(f"🚀 [{self.name}] 启动基于 Playwright 的无头节点采集引擎...")
        while True:
            try:
                # 执行一次完整的无头抓取和推送流程
                await self._execute_scrape_task()
            except Exception as e:
                print(f"❌ [{self.name}] 数据管道发生中断: {e}")

            # 引入抖动休眠 (Jitter Sleep)，模拟人类的不规则访问周期 (例如 3-5 分钟)
            # 极大降低被 Cloudflare 等 WAF 系统封控的概率
            sleep_time = random.randint(180, 300)
            print(f"💤 [{self.name}] 节点冷却中，等待 {sleep_time} 秒后开启下一轮下潜...\n")
            await asyncio.sleep(sleep_time)

    async def _execute_scrape_task(self):
        """执行单元：隔离的无头浏览器生命周期"""
        async with Stealth().use_async(async_playwright()) as p:
            # 生产环境下（尤其是在容器或服务器内），务必开启 headless=True
            browser = await p.chromium.launch(headless=True)

            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={'width': 1920, 'height': 1080}
            )
            page = await context.new_page()

            try:
                # 采用 domcontentloaded 提升响应速度，不再等待全量图片/广告加载
                await page.goto("https://cryptopanic.com/", wait_until="domcontentloaded")
                await page.wait_for_timeout(random.randint(4000, 6000))

                # 模拟轻度向下滚动，触发懒加载
                for _ in range(2):
                    await page.evaluate(f"window.scrollBy(0, {random.randint(600, 1000)})")
                    await page.wait_for_timeout(random.randint(2000, 3000))

                # 开始解析 DOM 树
                news_elements = await page.query_selector_all(".news-row")
                pushed_count = 0

                for element in news_elements:
                    try:
                        link_element = await element.query_selector("a.nc-title")
                        if not link_element: continue

                        title_element = await link_element.query_selector(".title-text > span:first-child")
                        title = await title_element.inner_text() if title_element else ""

                        source_element = await link_element.query_selector(".si-source-domain")
                        source = await source_element.inner_text() if source_element else "Unknown"

                        if title:
                            # ==========================================
                            # 核心枢纽：数据清洗与契约转换
                            # ==========================================
                            clean_title = title.strip()

                            # 1. 使用 TextCleaner 过滤黑名单，并提取资产标签 (假设你有 get_asset 方法)
                            # 如果包含 airdrop 等垃圾信息，可以直接在此处 continue 跳过
                            asset_tag = "UNKNOWN"
                            # asset_tag = self.cleaner.extract_asset(clean_title)

                            # 2. 严格遵循 SentimentData 契约组装数据
                            data = SentimentData(
                                asset=asset_tag,
                                raw_text=clean_title,
                                source=f"CryptoPanic-{source.strip()}",
                                timestamp=int(time.time())
                            )

                            # 3. 序列化为 JSON Bytes 并异步打入 Kafka 集群
                            # model_dump() 是 Pydantic v2 的标准用法（v1 为 dict()）
                            payload = json.dumps(data.model_dump()).encode("utf-8")
                            await self.producer.send_and_wait(self.topic, payload)

                            pushed_count += 1

                    except Exception:
                        # 生产环境：局部 DOM 解析失败不应引发全局崩溃，静默丢弃该条脏数据
                        pass

                print(f"✅ [{self.name}] 成功提取并推送 {pushed_count} 条标准结构化数据至 Kafka Topic: {self.topic}")

            finally:
                # 强保障：无论抓取成功还是网络异常断开，必须清理内存，防止僵尸进程撑爆服务器
                await context.close()
                await browser.close()
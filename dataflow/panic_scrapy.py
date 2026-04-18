import asyncio
import random
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

async def scrape_cryptopanic():
    async with Stealth().use_async(async_playwright()) as p:
        browser = await p.chromium.launch(headless=True)

        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={'width': 1920, 'height': 1080}
        )

        page = await context.new_page()

        print("🚀 正在建立与目标节点的连接...")
        try:
            await page.goto("https://cryptopanic.com/", wait_until="domcontentloaded")
            await page.wait_for_timeout(5000)

            # --- 核心排查 1：确认是否被风控拦截 ---
            page_title = await page.title()
            print(f"📌 当前所在页面 Title: {page_title}")

            if "Just a moment" in page_title or "Cloudflare" in page_title:
                print("⚠️ 警报：遭遇 Cloudflare 5秒盾拦截！")
                await page.screenshot(path="blocked_by_cf.png")
                # 提示：如果是简单的盾，Playwright 在 headless=False 下有时挂机十几秒能自动过
                await page.wait_for_timeout(10000)

            # --- 核心排查 2：保存现场快照 ---
            # 无论成功与否，拍个照确认目前屏幕上是什么
            await page.screenshot(path="current_view.png")

            for i in range(3):
                print(f"🔄 正在下潜抓取深层数据流... (第 {i+1} 页)")
                scroll_y = random.randint(600, 1500)
                await page.evaluate(f"window.scrollBy(0, {scroll_y})")
                await page.wait_for_timeout(random.randint(2000, 4000))

            # --- 进阶嗅探：采用多重降级匹配策略 ---
            # 策略A: 原生类名
            # 策略B: 寻找所有的外部链接包裹层 (CryptoPanic 特征)
            # 策略C: 匹配内部任何看起来像新闻标题的 a 标签层
            # 直接锁定新闻行
            news_elements = await page.query_selector_all(".news-row")

            results = []
            for element in news_elements:
                try:
                    # 1. 抓取包含核心信息的 a 标签
                    link_element = await element.query_selector("a.nc-title")
                    if not link_element:
                        continue

                    link = await link_element.get_attribute("href")

                    # 2. 深入 a 标签内部提取纯净的标题文本
                    title_element = await link_element.query_selector(".title-text > span:first-child")
                    title = await title_element.inner_text() if title_element else ""

                    # 3. 提取来源媒体
                    source_element = await link_element.query_selector(".si-source-domain")
                    source = await source_element.inner_text() if source_element else "Unknown"

                    # 4. 可选：提取关联的币种（如 BTC, ETH）
                    currency_element = await element.query_selector(".nc-currency a")
                    currency = await currency_element.inner_text() if currency_element else ""

                    if title and link:
                        results.append({
                            "title": title.strip(),
                            "link": f"https://cryptopanic.com{link}" if link.startswith("/") else link,
                            "source": source.strip(),
                            "currency": currency.strip()
                        })
                except Exception as e:
                    # 静默忽略解析异常的脏 DOM
                    pass

            # --- 核心排查 3：如果还是 0 条，把 HTML 源码抠出来 ---
            if len(results) == 0:
                print("❌ 数据流提取依然为空，正在保存 DOM 结构到本地...")
                html_content = await page.content()
                with open("error_dom.html", "w", encoding="utf-8") as f:
                    f.write(html_content)
                print("📁 DOM 已保存至 error_dom.html，请检查该文件找出最新的 CSS 选择器。")

            print(f"\n✅ 成功提取 {len(results)} 条数据 (预览前3条)：")
            for idx, item in enumerate(results[:3], 1):
                print(f"{idx}. {item['title']}\n   🔗 {item['link']}")

            return results

        except Exception as e:
            print(f"❌ 数据管道破裂: {e}")
        finally:
            await context.close()
            await browser.close()

if __name__ == "__main__":
    asyncio.run(scrape_cryptopanic())
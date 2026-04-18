import time
import requests
from pymilvus import connections, Collection
from datetime import datetime, timedelta

# --- 配置 ---
MILVUS_HOST = "localhost" # 或者你的容器地址
MILVUS_PORT = "19530"
COLLECTION_NAME = "eth_sentiment_analysis"

def get_eth_price():
    """获取币安现货 ETH/USDT 当前价格"""
    try:
        url = "https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDT"
        res = requests.get(url, timeout=5).json()
        return float(res['price'])
    except Exception as e:
        print(f"❌ 获取价格失败: {e}")
        return None

def settle_records():
    # 1. 连接 Milvus
    connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT)
    collection = Collection(COLLECTION_NAME)
    collection.load()

    # 2. 找到超过 24 小时且未结算的记录 (is_settled == false)
    # 计算 24 小时前的时间戳（毫秒）
    one_day_ago_ts = int((time.time() - 86400) * 1000)

    # 注意：Milvus 的查询需要定义输出字段
    res = collection.query(
        expr=f"is_settled == false and timestamp <= {one_day_ago_ts}",
        output_fields=["id", "price_at_t", "raw_content", "vector", "sentiment_score", "sentiment_reason", "timestamp"]
    )

    if not res:
        print(f"[{datetime.now()}] 没有需要结算的过期记录。")
        return

    current_price = get_eth_price()
    if not current_price: return

    print(f"🚀 开始结算 {len(res)} 条记录，当前 ETH 价格: {current_price}")

    updated_rows = []
    for row in res:
        price_at_t = row['price_at_t']

        # 计算 24h 收益率
        return_24h = (current_price - price_at_t) / price_at_t

        # 准备更新的数据 (Milvus 更新本质是 upsert，所以必须带上所有原始字段)
        updated_row = {
            "id": row["id"],
            "raw_content": row["raw_content"],
            "sentiment_score": row["sentiment_score"],
            "sentiment_reason": row["sentiment_reason"],
            "timestamp": row["timestamp"],
            "vector": row["vector"],
            "price_at_t": row["price_at_t"],
            "price_after_24h": current_price,
            "return_24h": return_24h,
            "is_settled": True  # 标记为已结算
        }
        updated_rows.append(updated_row)

    # 3. 执行批量更新 (Upsert)
    if updated_rows:
        collection.insert(updated_rows) # 同 ID 会覆盖，实现更新
        collection.flush()
        print(f"✅ 成功结算 {len(updated_rows)} 条记录。")

if __name__ == "__main__":
    while True:
        try:
            settle_records()
        except Exception as e:
            print(f"🛑 运行异常: {e}")

        # 每小时运行一次结算逻辑
        print("💤 等待 1 小时后进行下次结算...")
        time.sleep(3600)
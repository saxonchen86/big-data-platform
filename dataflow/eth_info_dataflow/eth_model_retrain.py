# 这个脚本将完成以下任务：
#
# 数据提取：从 Milvus 提取指定时间段内已结算（is_settled=true）的数据。
#
# 模型训练：使用 XGBoost（量化常用）训练一个从“向量”到“收益率”的回归模型。
#
# 模型保存：保存模型供 Flink 或 Python 决策逻辑加载。
#
# 数据清理：备份数据后，从 Milvus 中物理删除已训练的旧数据。
import time
import joblib
import numpy as np
import pandas as pd
import os
from pymilvus import connections, Collection
from xgboost import XGBRegressor
from datetime import datetime, timedelta

# --- 配置 ---
MILVUS_CONFIG = {"host": "localhost", "port": "19530"}
COLLECTION_NAME = "eth_sentiment_analysis"
MODEL_SAVE_PATH = "./models/eth_sentiment_xgb.joblib"
BACKUP_DIR = "./backups/"

# 确保模型和备份目录存在
os.makedirs("./models", exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)

def train_and_cleanup():
    # 1. 连接 Milvus
    connections.connect("default", **MILVUS_CONFIG)
    collection = Collection(COLLECTION_NAME)
    collection.load()

    # 2. 计算时间窗口（保留6个月，训练最老3个月）
    now = datetime.now()
    six_months_ago_ts = int((now - timedelta(days=180)).timestamp() * 1000)
    three_months_ago_ts = int((now - timedelta(days=90)).timestamp() * 1000)

    start_date = datetime.fromtimestamp(six_months_ago_ts/1000).strftime('%Y%m%d')
    end_date = datetime.fromtimestamp(three_months_ago_ts/1000).strftime('%Y%m%d')

    print(f"📅 目标训练周期: {start_date} -> {end_date}")

    # 3. 提取数据（增加 sentiment_score 字段）
    res = collection.query(
        expr=f"is_settled == true and timestamp >= {six_months_ago_ts} and timestamp < {three_months_ago_ts}",
        output_fields=["id", "vector", "sentiment_score", "return_24h", "raw_content", "timestamp"]
    )

    if len(res) < 100:
        print(f"⚠️ 样本量不足 ({len(res)})，建议积累更多数据后再训练。")
        return

    # 4. 数据备份（在删除前持久化到 Parquet 文件）
    df = pd.DataFrame(res)
    backup_file = f"{BACKUP_DIR}eth_data_backup_{start_date}_{end_date}.parquet"
    # Parquet 格式完美保留向量数组结构
    df.to_parquet(backup_file, engine='fastparquet')
    print(f"💾 数据已安全备份至: {backup_file}")

    # 5. 特征工程：拼接 [sentiment_score] + [vector]
    # 这样模型既能看到语义特征，也能看到直观的情绪评分
    X = []
    for row in res:
        score = float(row['sentiment_score'])
        vector = row['vector']
        # 将分数作为第一个特征，后面跟着 768 维向量，总维度 769
        combined_features = [score] + vector
        X.append(combined_features)

    X = np.array(X)
    y = np.array([row['return_24h'] for row in res])
    ids = [row['id'] for row in res]

    print(f"🚀 开始训练 XGBoost，特征维度: {X.shape[1]}，样本数: {len(X)}")

    # 6. 训练模型
    model = XGBRegressor(
        n_estimators=200,      # 增加迭代次数
        max_depth=6,
        learning_rate=0.05,
        objective='reg:squarederror',
        tree_method='hist'     # M4 Pro 跑这个极快
    )
    model.fit(X, y)

    # 7. 保存模型
    joblib.dump(model, MODEL_SAVE_PATH)
    print(f"✅ 模型训练完成并更新: {MODEL_SAVE_PATH}")

    # 8. 安全清理 Milvus 历史数据
    print(f"🧹 正在清理 Milvus 中的旧数据 ({len(ids)} 条)...")
    batch_size = 500
    for i in range(0, len(ids), batch_size):
        batch_ids = ids[i:i + batch_size]
        collection.delete(f"id in {batch_ids}")

    collection.flush()
    print("✨ 任务成功完成：备份 -> 训练 -> 清理。")

if __name__ == "__main__":
    start_t = time.time()
    try:
        train_and_cleanup()
    except Exception as e:
        print(f"❌ 运行失败: {e}")
    print(f"⏱️ 总耗时: {time.time() - start_t:.2f}s")
import requests
import json
from pymilvus import MilvusClient

# ================= 架构参数配置 =================
OLLAMA_URL = "http://localhost:11434/api/embeddings"
MILVUS_URI = "http://localhost:19530"
COLLECTION_NAME = "dofi_realtime_knowledge"
EMBEDDING_MODEL = "nomic-embed-text"

def get_embedding(text: str) -> list:
    """调用本地 Ollama 获取查询语句的向量"""
    payload = {
        "model": EMBEDDING_MODEL,
        "prompt": text
    }
    response = requests.post(OLLAMA_URL, json=payload)
    response.raise_for_status()
    return response.json()["embedding"]

def verify_pipeline():
    print("🚀 开始验证 Dofi 实时向量管道...")

    # 1. 构造我们的自然语言查询
    # 我们故意用与 Kafka 原文不同，但语义相近的句子进行测试
    query_text = "Find logs related to suspicious multiple login attempts or brute force attacks."
    print(f"\n[1] 正在对查询语句进行向量化: '{query_text}'")
    query_vector = get_embedding(query_text)

    # 2. 连接 Milvus 2.40.3
    print(f"\n[2] 连接 Milvus ({MILVUS_URI})...")
    client = MilvusClient(uri=MILVUS_URI)

    # 3. 执行向量检索
    print(f"\n[3] 在集合 '{COLLECTION_NAME}' 中执行 ANN 搜索...")
    search_res = client.search(
        collection_name=COLLECTION_NAME,
        data=[query_vector],
        limit=2,  # 返回最相似的 2 条记录
        search_params={"metric_type": "COSINE", "params": {"nprobe": 10}}, # nomic-embed 通常推荐 COSINE 或 IP
        output_fields=["raw_log"] # 要求 Milvus 返回我们插入的原始文本
    )

    # 4. 打印结果，验证语义匹配度
    print("\n✅ 检索结果:")
    for hits in search_res:
        for hit in hits:
            print(f"  ➜ 匹配度 (Distance/Score): {hit['distance']:.4f}")
            print(f"  ➜ 原始日志 (raw_log): {hit['entity']['raw_log']}\n")

if __name__ == "__main__":
    verify_pipeline()
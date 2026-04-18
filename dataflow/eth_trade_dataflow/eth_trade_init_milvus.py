import os
from pymilvus import (
    connections,
    FieldSchema, CollectionSchema, DataType,
    Collection, utility
)
from dotenv import load_dotenv

load_dotenv()

def create_eth_collection():
    # 1. 连接 Milvus
    host = os.getenv("MILVUS_HOST", "milvus-standalone")
    port = os.getenv("MILVUS_PORT", "19530")

    print(f"Connecting to Milvus at {host}:{port}...")
    connections.connect("default", host=host, port=port)

    collection_name = "eth_sentiment_analysis"

    # 2. 如果集合已存在，先删除（可选，生产环境请注释掉）
    if utility.has_collection(collection_name):
        # utility.drop_collection(collection_name)
        # print(f"Collection {collection_name} already exists.")
        pass

    # 3. 定义字段 Schema
    fields = [
        # event_id 作为主键，VarChar 类型，必须指定 max_length
        FieldSchema(name="event_id", dtype=DataType.VARCHAR, is_primary=True, max_length=128),

        FieldSchema(name="title", dtype=DataType.VARCHAR, max_length=512),
        FieldSchema(name="sentiment_score", dtype=DataType.INT64),
        FieldSchema(name="rsi_14", dtype=DataType.DOUBLE),
        FieldSchema(name="atr_14", dtype=DataType.DOUBLE),
        FieldSchema(name="price_at_t", dtype=DataType.DOUBLE),
        FieldSchema(name="pub_date", dtype=DataType.INT64),
        FieldSchema(name="sentiment_es", dtype=DataType.INT64),

        # 向量字段：768 维度 (nomic-embed-text)
        FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=768),

        FieldSchema(name="is_settled", dtype=DataType.BOOL, default_value=False),
        FieldSchema(name="win_rate", dtype=DataType.DOUBLE, default_value=0.0)
    ]

    schema = CollectionSchema(fields, description="ETH Sentiment Quant Trading Memory")

    # 4. 创建集合
    print(f"Creating collection: {collection_name}")
    collection = Collection(name=collection_name, schema=schema)

    # 5. 创建索引 (搜索必不可少)
    # 为向量字段创建索引，使用 IVF_FLAT 或 HNSW
    index_params = {
        "metric_type": "L2",      # 欧氏距离
        "index_type": "IVF_FLAT", # 倒排索引，适合中等规模
        "params": {"nlist": 128}
    }

    print("Creating index on 'vector' field...")
    collection.create_index(field_name="vector", index_params=index_params)

    # 6. 加载集合到内存
    collection.load()
    print(f"Collection {collection_name} is created and loaded.")

if __name__ == "__main__":
    create_eth_collection()
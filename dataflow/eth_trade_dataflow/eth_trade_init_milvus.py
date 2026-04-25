import os
from pymilvus import (
    connections,
    FieldSchema, CollectionSchema, DataType,
    Collection, utility
)
from dotenv import load_dotenv

load_dotenv()

def create_eth_collection():
    host = os.getenv("MILVUS_HOST", "localhost")
    port = os.getenv("MILVUS_PORT", "19530")

    print(f"Connecting to Milvus at {host}:{port}...")
    connections.connect("default", host=host, port=port)

    collection_name = "eth_sentiment_analysis"

    # 注意：因为增加了新字段，必须先删除旧的 collection 重新建表
    if utility.has_collection(collection_name):
        utility.drop_collection(collection_name)
        print(f"Collection {collection_name} dropped for schema update.")

    # 定义字段 Schema
    fields = [
        FieldSchema(name="event_id", dtype=DataType.VARCHAR, is_primary=True, max_length=128),
        FieldSchema(name="title", dtype=DataType.VARCHAR, max_length=512),
        FieldSchema(name="sentiment_score", dtype=DataType.INT64),
        FieldSchema(name="rsi_14", dtype=DataType.DOUBLE),
        FieldSchema(name="atr_14", dtype=DataType.DOUBLE),
        FieldSchema(name="price_at_t", dtype=DataType.DOUBLE),
        FieldSchema(name="pub_date", dtype=DataType.INT64),
        FieldSchema(name="sentiment_es", dtype=DataType.INT64),
        FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=768),
        FieldSchema(name="is_settled", dtype=DataType.BOOL, default_value=False),
        FieldSchema(name="win_rate", dtype=DataType.DOUBLE, default_value=0.0),

        # 新增：24小时历史回测收益率（Java 查询和提取所需的字段）
        FieldSchema(name="return", dtype=DataType.DOUBLE, default_value=0.0)
    ]

    schema = CollectionSchema(fields, description="ETH Sentiment Quant Trading Memory")

    print(f"Creating collection: {collection_name}")
    collection = Collection(name=collection_name, schema=schema)

    # 创建索引：注意将 L2 改为了 COSINE，以匹配 Java 侧 score > 0.9 的阈值逻辑
    index_params = {
        "metric_type": "COSINE",
        "index_type": "IVF_FLAT",
        "params": {"nlist": 128}
    }

    print("Creating index on 'vector' field...")
    collection.create_index(field_name="vector", index_params=index_params)

    collection.load()
    print(f"Collection {collection_name} is created and loaded.")

if __name__ == "__main__":
    create_eth_collection()
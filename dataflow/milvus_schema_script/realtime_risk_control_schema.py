from pymilvus import MilvusClient, DataType

# ================= 架构参数配置 =================
MILVUS_URI = "http://localhost:19530"
COLLECTION_NAME = "dofi_realtime_knowledge"
VECTOR_DIM = 768  # nomic-embed-text 的绝对维度
MAX_LOG_LENGTH = 8192  # 预留足够的日志文本长度

def init_dofi_knowledge_base():
    print("🔥 正在连接向量引擎...")
    client = MilvusClient(uri=MILVUS_URI)

    # 1. 如果存在旧表，先进行清理（防止 Schema 冲突）
    if client.has_collection(collection_name=COLLECTION_NAME):
        print(f"⚠️ 检测到旧版本集合 '{COLLECTION_NAME}'，正在将其抹除...")
        client.drop_collection(collection_name=COLLECTION_NAME)

    # 2. 构造工业级 Schema
    print("📐 正在构建 Schema...")
    # enable_dynamic_field=True 是高级玩法：允许后续随意插入未定义的 JSON 字段，扩展性拉满
    schema = MilvusClient.create_schema(
        auto_id=True,
        enable_dynamic_field=True
    )

    # 物理字段定义
    # Flink 不需要传 ID，Milvus 会用雪花算法自动生成，彻底避免分布式并发写入时的 ID 冲突
    schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
    schema.add_field(field_name="raw_log", datatype=DataType.VARCHAR, max_length=MAX_LOG_LENGTH)
    schema.add_field(field_name="vector", datatype=DataType.FLOAT_VECTOR, dim=VECTOR_DIM)

    # 3. 配置针对实时 RAG 的顶配索引 (HNSW)
    print("🧠 正在配置 HNSW 内存图索引...")
    index_params = client.prepare_index_params()

    index_params.add_index(
        field_name="vector",
        metric_type="COSINE",  # 语义检索标准距离度量
        index_type="HNSW",     # 分层导航小世界算法，性能最炸裂的图索引
        index_name="vector_index",
        params={
            "M": 16,               # 节点最大边数，值越大查询越准但占内存，16是平衡点
            "efConstruction": 200  # 构建索引时的搜索深度
        }
    )

    # 4. 提交物理建表
    print("🔨 正在将物理结构映射到存储层...")
    client.create_collection(
        collection_name=COLLECTION_NAME,
        schema=schema,
        index_params=index_params,
        consistency_level="Bounded" # 核心调优：允许极小的主从延迟，极大提升高吞吐写入性能
    )

    # 5. 将集合加载到 M4 Pro 的统一内存中，准备接客
    client.load_collection(collection_name=COLLECTION_NAME)

    print(f"✅ 成功！集合 '{COLLECTION_NAME}' 已准备就绪，可以承接 Flink 洪流了。")

if __name__ == "__main__":
    init_dofi_knowledge_base()
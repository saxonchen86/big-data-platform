import asyncio
import json
import logging
import os
from typing import List, Dict, Any, Optional

from aiokafka import AIOKafkaProducer

from web3 import AsyncWeb3
from web3.providers.websocket import WebsocketProviderV2

# 配置工业级高并发日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d | %(name)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("DataSensingLayer")

class KafkaStreamInterface:
    """
    Kafka 真实投递接口层
    按 SOP 规范要求，必须以特定 Partition Key 写入以确保局部有序性
    目前采用 JSON 序列化，后续可升级为 Protobuf/Avro 进行极速序列化
    """
    def __init__(self, bootstrap_servers: str):
        self.servers = bootstrap_servers
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.servers,
            acks="all"  # 金融级容灾：强确认
        )

    async def connect(self):
        logger.info(f"[Kafka] Connecting to brokers at {self.servers}")
        await self.producer.start()

    async def emit(self, topic: str, partition_key: str, payload: dict):
        """
        向 Flink 下游极速投递特征数据
        """
        try:
            # 序列化并投递
            serialized_data = json.dumps(payload).encode('utf-8')
            await self.producer.send_and_wait(
                topic,
                key=partition_key.encode('utf-8'),
                value=serialized_data
            )
            # 改为 INFO 级别以便在控制台直接看到效果
            logger.info(f"[Kafka] ✅ Emit -> {topic} | Key: {partition_key} | Payload: {serialized_data[:80]}...")
        except Exception as e:
            logger.error(f"[Kafka] ❌ Failed to emit to {topic}: {e}")


class ChainSensingNode:
    """
    链上数据采集核心骨架 (Data Ingestion & Sensory Input)
    支持多节点冗余路由、高并发异步 WSS 处理、Mempool 监听与 DEX 事件捕获。
    """
    def __init__(self, rpc_urls: List[str], target_pools: List[str], kafka_client: KafkaStreamInterface):
        self.rpc_urls = rpc_urls
        self.target_pools = target_pools
        self.kafka = kafka_client
        self.current_rpc_idx = 0

        # 预计算底层 Topic 的 Keccak256 Hash
        # Swap(address,address,int256,int256,uint160,uint128,int24)
        self.SWAP_TOPIC = AsyncWeb3.keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24)").hex()
        # Sync(uint112,uint112)
        self.SYNC_TOPIC = AsyncWeb3.keccak(text="Sync(uint112,uint112)").hex()

    async def run_forever(self):
        """核心重连与节点冗余路由守卫 (Node Routing)"""
        while True:
            url = self.rpc_urls[self.current_rpc_idx]
            logger.info(f"[Network] Attempting to connect WSS Node: {url}")
            try:
                # 采用 Web3.py V6+ 推荐的 persistent_websocket 范式处理持久化连接
                async with AsyncWeb3.persistent_websocket(WebsocketProviderV2(url)) as w3:
                    logger.info(f"[Network] ✅ WSS Connected: {url}")

                    # 订阅 1: Mempool 黑暗森林 (newPendingTransactions)
                    await w3.eth.subscribe('newPendingTransactions')

                    # 订阅 2: DEX Event Logs (利用 Bloom Filter 精准捕获目标池事件)
                    filter_params = {
                        "address": self.target_pools,
                        "topics": [[self.SWAP_TOPIC, self.SYNC_TOPIC]]
                    }
                    await w3.eth.subscribe('logs', filter_params)

                    logger.info("[Sensing] Subscriptions active. Listening to data streams...")

                    # 异步迭代合并的 WebSocket 消息流，实现高并发无阻塞分发
                    async for message in w3.ws.process_subscriptions():
                        asyncio.create_task(self._route_message(w3, message))

            except Exception as e:
                logger.error(f"[Network] ❌ Connection dropped or failed ({url}): {e}")

            # 容灾降级：切换到备用节点并进行指数退避
            self.current_rpc_idx = (self.current_rpc_idx + 1) % len(self.rpc_urls)
            logger.warning("[Network] Failing over to next RPC node in 2 seconds...")
            await asyncio.sleep(2)

    async def _route_message(self, w3: AsyncWeb3, message: dict):
        """路由并极速反序列化来自 WSS 的推送"""
        try:
            result = message.get("result")
            if not result:
                return

            # 区分消息类型：Tx Hash (Mempool 订阅) 还是 Log 字典 (Events 订阅)
            if isinstance(result, (str, bytes)):
                await self._process_mempool_tx(w3, result)
            elif isinstance(result, dict) and "topics" in result:
                await self._process_dex_log(result)
        except Exception as e:
            logger.error(f"[Sensing] Error routing message: {e}")

    async def _process_mempool_tx(self, w3: AsyncWeb3, tx_hash: str):
        """
        高并发反序列化解析 Gas Price / To Address
        为大模型 AI 预判 MEV、抢跑提供“未确认”视野
        """
        try:
            # ⚠️ 架构师提示: 此处为概念骨架。在极高并发的生产环境中，频繁调用 get_transaction 会拖垮 RPC 节点。
            # 生产级最佳实践: 使用 Erigon/Reth 自建节点并开启 full pending transaction (含 body) 订阅，
            # 或直接在 C/Rust 层使用轻量级 RLP 解码 raw payload 绕过 Web3.py 封包。
            tx = await w3.eth.get_transaction(tx_hash)
            if not tx:
                return

            to_addr = tx.get('to')
            if not to_addr:
                return  # 过滤纯合约创建交易

            payload = {
                "hash": tx_hash.hex() if isinstance(tx_hash, bytes) else tx_hash,
                "to": to_addr,
                "gasPrice": tx.get('gasPrice'),
                "maxPriorityFeePerGas": tx.get('maxPriorityFeePerGas'),
                # 将 input data 抛给下游 Flink 和大模型解析具体的 Swap 意图
                "input": tx.get('input').hex() if tx.get('input') else "0x"
            }

            # 按 SOP 要求，以接收方合约地址 Hash 为 Partition Key
            await self.kafka.emit(topic="hunter_mempool_raw", partition_key=str(to_addr), payload=payload)
        except Exception:
            # Pending Tx 可能瞬间被打包或丢弃，导致 get_transaction 找不到并抛错，容错忽略即可
            pass

    async def _process_dex_log(self, log: dict):
        """
        解析 DEX 的 Swap/Sync/Mint/Burn 等事件变动
        为 Flink 的 CEP 流动性突变匹配提供实时状态流
        """
        try:
            contract_addr = log.get('address')
            topics = [t.hex() if isinstance(t, bytes) else t for t in log.get('topics', [])]

            payload = {
                "address": contract_addr,
                "topics": topics,
                "data": log.get('data').hex() if isinstance(log.get('data'), bytes) else log.get('data'),
                "transactionHash": log.get('transactionHash').hex() if isinstance(log.get('transactionHash'), bytes) else log.get('transactionHash'),
                "blockNumber": log.get('blockNumber'),
                "logIndex": log.get('logIndex')
            }

            # 严格以 Pool 合约地址作为 Partition Key，保证 Flink 侧聚合的状态一致性
            await self.kafka.emit(topic="hunter_dex_logs_raw", partition_key=contract_addr, payload=payload)
        except Exception as e:
            logger.error(f"[Sensing] Failed to process log: {e}")


async def main():
    # ==== 节点资源池与配置中心 ====
    # 模拟从环境变量或配置中心拉取 (主备冗余 + 兜底归档节点)
    RPC_NODES = [
        os.getenv("RPC_NODE_PRIMARY", "wss://eth-mainnet.alchemyapi.io/v2/YOUR_ALCHEMY_KEY"),
        os.getenv("RPC_NODE_SECONDARY", "wss://wildcard.infura.io/ws/v3/YOUR_INFURA_KEY"),
        os.getenv("RPC_NODE_ARCHIVE", "ws://127.0.0.1:8546")
    ]

    # 目标监控 DEX 资金池 (例如: Uniswap V3 USDC/WETH Pool)
    TARGET_POOLS = [
        "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
    ]

    KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")

    # ==== 依赖注入 (IoC) ====
    kafka_interface = KafkaStreamInterface(bootstrap_servers=KAFKA_BROKERS)
    await kafka_interface.connect()

    collector = ChainSensingNode(
        rpc_urls=RPC_NODES,
        target_pools=TARGET_POOLS,
        kafka_client=kafka_interface
    )

    # ==== 守护进程启动 ====
    logger.info("🚀 System Booting: On-Chain Dynamic Hunter - Data Sensing Layer")
    await collector.run_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Collector shutdown by user.")

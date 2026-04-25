import os
import time
import logging
import ccxt
import pandas as pd
import pandas_ta as ta
import mysql.connector
from datetime import datetime
import pytz
from dotenv import load_dotenv

# 设置上海时区
TZ_SHANGHAI = pytz.timezone('Asia/Shanghai')

# 日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("MarketData")

load_dotenv()

class MarketDataCollector:
    def __init__(self):
        # 1. 初始化交易所 (使用币安公共接口)
        self.exchange = ccxt.binance({
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })

        # 2. 初始化数据库连接
        self.db = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST", "localhost"),
            user=os.getenv("MYSQL_USER", "root"),
            password=os.getenv("MYSQL_PASSWORD", "streampark"),
            database=os.getenv("MYSQL_DATABASE", "trade")
        )
        self.cursor = self.db.cursor()

    def fetch_and_calculate(self, symbol="ETH/USDT", limit=100):
        try:
            # 1. 获取 OHLCV 数据 (1分钟K线)
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe='1m', limit=limit)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

            # 2. 计算技术指标
            # RSI 14
            df['rsi_14'] = ta.rsi(df['close'], length=14)
            # ATR 14
            df['atr_14'] = ta.atr(df['high'], df['low'], df['close'], length=14)

            # 3. 准备写入
            # 过滤掉指标计算初期的 NaN 值
            df = df.dropna()

            sql = """
                  REPLACE INTO trade.eth_kline_features
                      (timestamp, datetime_sh, price_at_t, rsi_14, atr_14, high, low, close)
                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s) \
                  """

            records = []
            for _, row in df.iterrows():
                # 注意：ccxt 返回的时间戳已经是 UTC 毫秒，我们需要将其转为上海时区 datetime 存储以供直观核查
                dt_object = datetime.fromtimestamp(row['timestamp'] / 1000, TZ_SHANGHAI)

                records.append((
                    int(row['timestamp']),
                    dt_object.strftime('%Y-%m-%d %H:%M:%S'),
                    float(row['close']),
                    float(row['rsi_14']),
                    float(row['atr_14']),
                    float(row['high']),
                    float(row['low']),
                    float(row['close'])
                ))

            # 4. 执行批量写入
            self.cursor.executemany(sql, records)
            self.db.commit()

            logger.info(f"Successfully processed {len(records)} records for {symbol}")

        except Exception as e:
            logger.error(f"Error fetching market data: {str(e)}")
            self.db.rollback()

    def run_forever(self):
        logger.info("Market data collector started (Interval: 60s)...")
        while True:
            self.fetch_and_calculate()
            # 每分钟运行一次
            time.sleep(60)

if __name__ == "__main__":
    collector = MarketDataCollector()
    # 首次运行先获取 500 条以确保 RSI 计算准确性
    collector.fetch_and_calculate(limit=500)
    collector.run_forever()
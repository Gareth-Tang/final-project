import os
import yfinance as yf
import pandas as pd
import sqlite3
from dotenv import load_dotenv
from datetime import datetime
import time
import logging
from retry import retry

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='stock_crawler.log'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

# 加载环境变量
load_dotenv()

# 数据库配置
SP500_DB_PATH = os.getenv('SP500_DB_PATH', 'finance_portfolio_sp500.db')
PRIORITY_DB_PATH = os.getenv('PRIORITY_DB_PATH', 'finance_portfolio_priority.db')

# 爬取配置
CRAWL_CONFIG = {
    'batch_size': 50,         # 每批处理的股票数量
    'request_delay': 1.5,     # 每次请求间隔(秒)
    'batch_delay': 10,        # 每批处理后的延迟(秒)
    'retry_attempts': 3,      # 失败重试次数
    'retry_delay': 5          # 重试间隔(秒)
}

# 预定义的重点股票列表
PRIORITY_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA", "BRK-B", "JPM", "JNJ",
    "PG", "XOM", "V", "UNH", "HD", "BAC", "MA", "PFE", "DIS", "ADBE", "NFLX", "CRM",
    "CMCSA", "COST", "PEP", "AVGO", "CSCO", "TMO", "ABBV", "ACN", "LLY", "BMY", "DHR",
    "LIN", "TXN", "NKE", "UPS", "WMT", "MRK", "RTX", "HON", "PM", "UNP", "ORCL", "AMD",
    "QCOM", "SBUX", "CVS", "LOW", "GS", "IBM", "AMGN", "CAT", "GE", "INTC", "MMM", "BA",
    "MS", "BLK", "C", "AXP", "GILD", "MDLZ", "BKNG", "ADP", "MO", "AMT", "CI", "T", "CME",
    "CHTR", "COP", "SPGI", "ISRG", "LMT", "SYK", "VRTX", "ADI", "MDT", "TJX", "BDX", "TMUS",
    "DUK", "CB", "ZTS", "PYPL", "REGN", "SO", "VRTX", "AON", "USB", "EQIX", "D", "NOC", "ETN",
    "MCD", "DE", "CL", "ANTM", "HUM", "EL", "AEP", "WM", "PNC", "GD", "CCI", "CSX", "FISV",
    "HAL", "INTU", "ITW", "LRCX", "MMC", "NOW", "PSA", "SRE", "WM", "XEL", "AIG", "ALL", "BAX",
    "BIIB", "BSX", "CMG", "COP", "CTAS", "CTSH", "DOW", "DTE", "EA", "EMR", "EXC", "F", "FDX",
    "GD", "GIS", "HCA", "HLT", "HSY", "ICE", "IDXX", "INCY", "JCI", "KDP", "KHC", "KR", "LHX",
    "LUV", "MAR", "MCK", "MET", "MNST", "MOS", "MRVL", "MSI", "NDAQ", "NEM", "NI", "NUE", "OXY",
    "PAYX", "PCAR", "PEG", "PH", "PXD", "PNR", "PPG", "PRU", "PYPL", "RE", "ROST", "SBUX", "SCHW",
    "SO", "STZ", "TGT", "TRV", "TSN", "UAL", "UDR", "UPS", "URI", "VLO", "WBA", "WEC", "WFC", "WY",
    "XEL", "XLNX", "XYL", "YUM", "ZBH", "ZION", "A", "AAL", "AAP", "ABT", "ADM", "AEE", "AEP", "AES",
    "AFL", "AIG", "AIZ", "AJG", "AKAM", "ALB", "ALGN", "ALK", "ALL", "ALLE", "ALXN", "AMAT", "AMP", "AMT",
    "AMZN", "ANET", "ANSS", "ANTM", "AON", "AOS", "APA", "APC", "APD", "APH", "APTV", "ARE", "ATO", "ATVI",
    "AVB", "AVGO", "AVY", "AWK", "AXP", "AZO", "BA", "BAC", "BAX", "BBY", "BDX", "BEN", "BF-B", "BIIB", "BK",
    "BKNG", "BLK", "BLL", "BMY", "BR", "BRK-B", "BSX", "BWA", "BXP", "C", "CAG", "CAH", "CAT", "CB", "CBOE", "CBRE",
    "CCI", "CCL", "CDNS", "CDW", "CE", "CELG", "CERN", "CF", "CFG", "CHD", "CHRW", "CHTR", "CI", "CINF", "CL", "CLX",
    "CMA", "CMCSA", "CME", "CMG", "CMI", "CMS", "CNC", "CNP", "COF", "COG", "COO", "COP", "COST", "CPB", "CPRT", "CRM",
    "CSCO", "CSX", "CTAS", "CTL", "CTSH", "CTVA", "CVS", "CVX", "CXO", "D", "DAL", "DD", "DE", "DFS", "DG", "DGX", "DHI"
]

def create_sp500_connection():
    """创建标普500数据库连接"""
    try:
        connection = sqlite3.connect(SP500_DB_PATH)
        logging.info(f"标普500数据库连接成功: {SP500_DB_PATH}")
        return connection
    except Exception as e:
        logging.error(f"标普500数据库连接错误: {e}")
        raise

def create_priority_connection():
    """创建重点股票数据库连接"""
    try:
        connection = sqlite3.connect(PRIORITY_DB_PATH)
        logging.info(f"重点股票数据库连接成功: {PRIORITY_DB_PATH}")
        return connection
    except Exception as e:
        logging.error(f"重点股票数据库连接错误: {e}")
        raise

def init_sp500_database():
    """初始化标普500数据库表结构"""
    try:
        connection = create_sp500_connection()
        cursor = connection.cursor()
        
        # 创建资产主表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Assets (
            asset_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker_symbol TEXT UNIQUE,
            name TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            currency TEXT DEFAULT 'USD'
        )
        """)
        
        # 创建价格历史表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS PriceHistory (
            history_id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id INTEGER NOT NULL,
            date DATE NOT NULL,
            open_price REAL,
            high_price REAL,
            low_price REAL,
            close_price REAL,
            volume INTEGER,
            FOREIGN KEY (asset_id) REFERENCES Assets(asset_id),
            UNIQUE (asset_id, date)
        )
        """)
        
        # 创建索引以提高查询性能
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_assets_ticker ON Assets(ticker_symbol)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_history_asset_date ON PriceHistory(asset_id, date)")
        
        connection.commit()
        logging.info("标普500数据库表初始化完成")
    except Exception as e:
        logging.error(f"创建标普500数据库表时出错: {e}")
        raise
    finally:
        if connection:
            connection.close()

def init_priority_database():
    """初始化重点股票数据库表结构"""
    try:
        connection = create_priority_connection()
        cursor = connection.cursor()
        
        # 创建资产主表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Assets (
            asset_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker_symbol TEXT UNIQUE,
            name TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            currency TEXT DEFAULT 'USD'
        )
        """)
        
        # 创建价格历史表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS PriceHistory (
            history_id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id INTEGER NOT NULL,
            date DATE NOT NULL,
            open_price REAL,
            high_price REAL,
            low_price REAL,
            close_price REAL,
            volume INTEGER,
            FOREIGN KEY (asset_id) REFERENCES Assets(asset_id),
            UNIQUE (asset_id, date)
        )
        """)
        
        # 创建索引以提高查询性能
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_assets_ticker ON Assets(ticker_symbol)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_history_asset_date ON PriceHistory(asset_id, date)")
        
        connection.commit()
        logging.info("重点股票数据库表初始化完成")
    except Exception as e:
        logging.error(f"创建重点股票数据库表时出错: {e}")
        raise
    finally:
        if connection:
            connection.close()

def get_sp500_tickers():
    """获取标普500成分股列表"""
    try:
        logging.info("正在获取标普500成分股列表...")
        table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        df = table[0]
        tickers = df['Symbol'].tolist()
        
        # 处理特殊符号
        tickers = [ticker.replace('.', '-') for ticker in tickers]
        
        logging.info(f"成功获取 {len(tickers)} 只标普500成分股")
        return tickers
    except Exception as e:
        logging.error(f"获取标普500成分股失败: {e}")
        return []

@retry(tries=CRAWL_CONFIG['retry_attempts'], delay=CRAWL_CONFIG['retry_delay'])
def fetch_and_store_sp500_data(ticker):
    """获取标普500成分股数据并存储到数据库"""
    try:
        # 获取资产数据
        asset = yf.Ticker(ticker)
        info = asset.info
        
        # 获取历史价格数据
        hist = asset.history(period="30d")
        
        if hist.empty:
            logging.warning(f"❌ {ticker} 没有可用的历史价格数据")
            return False
        
        connection = create_sp500_connection()
        cursor = connection.cursor()
        
        # 插入或更新资产主表
        try:
            cursor.execute(
                """
                INSERT OR REPLACE INTO Assets 
                (ticker_symbol, name, asset_type, currency)
                VALUES (?, ?, ?, ?)
                """,
                (
                    ticker,
                    info.get('longName', f'{ticker} Inc.'),
                    'stock',
                    info.get('currency', 'USD')
                )
            )
            
            # 获取插入的asset_id
            cursor.execute("SELECT asset_id FROM Assets WHERE ticker_symbol = ?", (ticker,))
            asset_id = cursor.fetchone()[0]
            
            # 批量插入价格历史数据
            price_records = []
            for date, row in hist.iterrows():
                price_records.append((
                    asset_id,
                    date.date(),
                    row['Open'],
                    row['High'],
                    row['Low'],
                    row['Close'],
                    row['Volume']
                ))
            
            cursor.executemany(
                """
                INSERT OR REPLACE INTO PriceHistory 
                (asset_id, date, open_price, high_price, low_price, close_price, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                price_records
            )
            
            connection.commit()
            logging.info(f"✅ 标普500成分股 {ticker} 数据已成功更新（{len(price_records)} 天价格数据）")
            return True
            
        except Exception as e:
            connection.rollback()
            logging.error(f"❌ 存储标普500成分股 {ticker} 数据时数据库操作失败: {e}")
            return False
        finally:
            if connection:
                connection.close()
                
    except Exception as e:
        logging.error(f"❌ 获取或存储标普500成分股 {ticker} 数据时出错: {e}")
        raise

@retry(tries=CRAWL_CONFIG['retry_attempts'], delay=CRAWL_CONFIG['retry_delay'])
def fetch_and_store_priority_data(ticker):
    """获取重点股票数据并存储到数据库"""
    try:
        # 获取资产数据
        asset = yf.Ticker(ticker)
        info = asset.info
        
        # 获取历史价格数据
        hist = asset.history(period="30d")
        
        if hist.empty:
            logging.warning(f"❌ {ticker} 没有可用的历史价格数据")
            return False
        
        connection = create_priority_connection()
        cursor = connection.cursor()
        
        # 插入或更新资产主表
        try:
            cursor.execute(
                """
                INSERT OR REPLACE INTO Assets 
                (ticker_symbol, name, asset_type, currency)
                VALUES (?, ?, ?, ?)
                """,
                (
                    ticker,
                    info.get('longName', f'{ticker} Inc.'),
                    'stock',
                    info.get('currency', 'USD')
                )
            )
            
            # 获取插入的asset_id
            cursor.execute("SELECT asset_id FROM Assets WHERE ticker_symbol = ?", (ticker,))
            asset_id = cursor.fetchone()[0]
            
            # 批量插入价格历史数据
            price_records = []
            for date, row in hist.iterrows():
                price_records.append((
                    asset_id,
                    date.date(),
                    row['Open'],
                    row['High'],
                    row['Low'],
                    row['Close'],
                    row['Volume']
                ))
            
            cursor.executemany(
                """
                INSERT OR REPLACE INTO PriceHistory 
                (asset_id, date, open_price, high_price, low_price, close_price, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                price_records
            )
            
            connection.commit()
            logging.info(f"✅ 重点股票 {ticker} 数据已成功更新（{len(price_records)} 天价格数据）")
            return True
            
        except Exception as e:
            connection.rollback()
            logging.error(f"❌ 存储重点股票 {ticker} 数据时数据库操作失败: {e}")
            return False
        finally:
            if connection:
                connection.close()
                
    except Exception as e:
        logging.error(f"❌ 获取或存储重点股票 {ticker} 数据时出错: {e}")
        raise

def fetch_all_assets_in_batches(tickers, fetch_function, batch_name, db_path):
    """分批获取所有股票数据"""
    if not tickers:
        logging.warning(f"没有提供 {batch_name} 股票列表")
        return 0, []
    
    total_batches = (len(tickers) + CRAWL_CONFIG['batch_size'] - 1) // CRAWL_CONFIG['batch_size']
    success_count = 0
    failed_tickers = []
    
    logging.info(f"开始爬取 {len(tickers)} 只{batch_name}股票数据，共 {total_batches} 批...")
    logging.info(f"{batch_name}股票数据将存储到: {db_path}")
    
    for batch_num in range(total_batches):
        start_idx = batch_num * CRAWL_CONFIG['batch_size']
        end_idx = min((batch_num + 1) * CRAWL_CONFIG['batch_size'], len(tickers))
        batch_tickers = tickers[start_idx:end_idx]
        
        logging.info(f"\n=== 处理第 {batch_num+1}/{total_batches} 批{batch_name}股票，共 {len(batch_tickers)} 只 ===")
        
        for i, ticker in enumerate(batch_tickers, 1):
            logging.info(f"({i}/{len(batch_tickers)}) 处理: {ticker}")
            try:
                if fetch_function(ticker):
                    success_count += 1
            except Exception as e:
                failed_tickers.append(ticker)
            
            # 请求间隔
            if i < len(batch_tickers):
                time.sleep(CRAWL_CONFIG['request_delay'])
        
        # 批次间延迟
        if batch_num < total_batches - 1:
            logging.info(f"批次处理完成，等待 {CRAWL_CONFIG['batch_delay']} 秒后继续...")
            time.sleep(CRAWL_CONFIG['batch_delay'])
    
    # 输出结果统计
    logging.info(f"\n===== {batch_name}股票爬取完成 =====")
    logging.info(f"总股票数: {len(tickers)}")
    logging.info(f"成功: {success_count}")
    logging.info(f"失败: {len(failed_tickers)}")
    
    if failed_tickers:
        logging.info(f"失败的股票: {failed_tickers}")
        with open(f'failed_{batch_name.lower().replace(" ", "_")}_tickers.txt', 'w') as f:
            f.write('\n'.join(failed_tickers))
    
    return success_count, failed_tickers

if __name__ == "__main__":
    logging.info("===== 股票数据爬取程序启动 =====")
    
    try:
        # 初始化数据库
        init_sp500_database()
        init_priority_database()
        
        # 获取标普500成分股列表
        sp500_tickers = get_sp500_tickers()
        
        # 爬取标普500数据
        sp500_success, sp500_failed = fetch_all_assets_in_batches(
            sp500_tickers, 
            fetch_and_store_sp500_data, 
            "标普500",
            SP500_DB_PATH
        )
        
        # 爬取重点股票数据
        priority_success, priority_failed = fetch_all_assets_in_batches(
            PRIORITY_TICKERS, 
            fetch_and_store_priority_data, 
            "重点",
            PRIORITY_DB_PATH
        )
        
        logging.info(f"标普500数据已成功保存到: {SP500_DB_PATH}")
        logging.info(f"重点股票数据已成功保存到: {PRIORITY_DB_PATH}")
        logging.info("===== 程序运行完成 =====")
        
    except Exception as e:
        logging.critical(f"程序运行出错: {e}", exc_info=True)
    finally:
        logging.info("程序已退出")
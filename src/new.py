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
DB_PATH = os.getenv('DB_PATH', 'finance_portfolio.db')

# 爬取配置
CRAWL_CONFIG = {
    'batch_size': 50,         # 每批处理的股票数量
    'request_delay': 1.5,     # 每次请求间隔(秒)
    'batch_delay': 10,        # 每批处理后的延迟(秒)
    'retry_attempts': 3,      # 失败重试次数
    'retry_delay': 5          # 重试间隔(秒)
}

def create_connection():
    """创建数据库连接"""
    try:
        connection = sqlite3.connect(DB_PATH)
        logging.info(f"数据库连接成功: {DB_PATH}")
        return connection
    except Exception as e:
        logging.error(f"数据库连接错误: {e}")
        raise

def init_database():
    """初始化数据库表结构"""
    try:
        connection = create_connection()
        cursor = connection.cursor()
        
        # 创建表
        create_table_query = """
        CREATE TABLE IF NOT EXISTS Assets (
            asset_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker_symbol TEXT UNIQUE,
            name TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            current_price REAL,
            percent_change_today REAL,
            price_updated_at TIMESTAMP,
            currency TEXT DEFAULT 'USD'
        )
        """
        cursor.execute(create_table_query)
        connection.commit()
        logging.info("数据库表初始化完成")
    except Exception as e:
        logging.error(f"创建表时出错: {e}")
        raise
    finally:
        if connection:
            connection.close()

def get_sp500_tickers():
    """获取标普500成分股列表（使用备用数据源）"""
    try:
        # 尝试从维基百科获取
        logging.info("正在获取标普500成分股列表...")
        table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        df = table[0]
        tickers = df['Symbol'].tolist()
        
        # 处理特殊符号
        tickers = [ticker.replace('.', '-') for ticker in tickers]
        
        logging.info(f"成功获取 {len(tickers)} 只标普500成分股")
        return tickers
    except Exception as e:
        logging.error(f"从维基百科获取成分股失败: {e}")
        logging.info("使用预定义的标普500成分股列表...")
        
        # 使用预定义的标普500成分股子集（约500只）
        return [
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

@retry(tries=CRAWL_CONFIG['retry_attempts'], delay=CRAWL_CONFIG['retry_delay'])
def fetch_and_store_asset_data(ticker):
    """获取资产数据并存储到数据库（带重试机制）"""
    try:
        # 获取资产数据
        asset = yf.Ticker(ticker)
        info = asset.info
        
        # 提取需要的数据
        data = {
            'ticker_symbol': ticker,
            'name': info.get('longName', f'{ticker} Inc.'),
            'asset_type': 'stock',
            'current_price': info.get('currentPrice'),
            'percent_change_today': info.get('regularMarketChangePercent'),
            'price_updated_at': datetime.now(),
            'currency': info.get('currency', 'USD')
        }
        
        # 存储到数据库
        connection = create_connection()
        cursor = connection.cursor()
        
        # 使用 REPLACE INTO 处理重复数据
        replace_query = """
        REPLACE INTO Assets 
            (ticker_symbol, name, asset_type, current_price, 
             percent_change_today, price_updated_at, currency)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(replace_query, tuple(data.values()))
        connection.commit()
        connection.close()
        
        logging.info(f"✅ {ticker} ({data['name']}) 数据已成功更新")
        return True
        
    except Exception as e:
        logging.error(f"❌ 获取或存储 {ticker} 数据时出错: {e}")
        raise

def fetch_all_assets_in_batches(tickers):
    """分批获取所有股票数据"""
    total_batches = (len(tickers) + CRAWL_CONFIG['batch_size'] - 1) // CRAWL_CONFIG['batch_size']
    success_count = 0
    failed_tickers = []
    
    logging.info(f"开始爬取 {len(tickers)} 只股票数据，共 {total_batches} 批...")
    
    for batch_num in range(total_batches):
        start_idx = batch_num * CRAWL_CONFIG['batch_size']
        end_idx = min((batch_num + 1) * CRAWL_CONFIG['batch_size'], len(tickers))
        batch_tickers = tickers[start_idx:end_idx]
        
        logging.info(f"\n=== 处理第 {batch_num+1}/{total_batches} 批，共 {len(batch_tickers)} 只股票 ===")
        
        for i, ticker in enumerate(batch_tickers, 1):
            logging.info(f"({i}/{len(batch_tickers)}) 处理: {ticker}")
            try:
                if fetch_and_store_asset_data(ticker):
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
    logging.info(f"\n===== 爬取完成 =====")
    logging.info(f"总股票数: {len(tickers)}")
    logging.info(f"成功: {success_count}")
    logging.info(f"失败: {len(failed_tickers)}")
    
    if failed_tickers:
        logging.info(f"失败的股票: {failed_tickers}")
        with open('failed_tickers.txt', 'w') as f:
            f.write('\n'.join(failed_tickers))
    
    return success_count, failed_tickers

if __name__ == "__main__":
    logging.info("===== 股票数据爬取程序启动 =====")
    
    try:
        # 初始化数据库
        init_database()
        
        # 获取标普500成分股列表
        tickers = get_sp500_tickers()
        
        # 爬取所有股票数据
        success_count, failed_tickers = fetch_all_assets_in_batches(tickers)
        
        logging.info(f"数据已成功保存到: {DB_PATH}")
        logging.info("===== 程序运行完成 =====")
        
    except Exception as e:
        logging.critical(f"程序运行出错: {e}", exc_info=True)
    finally:
        logging.info("程序已退出")
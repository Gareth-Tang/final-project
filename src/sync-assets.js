const mysql = require('mysql2/promise');
const axios = require('axios');
const cheerio = require('cheerio');
const https = require('https');

// 1. 全局配置
const httpsAgent = new https.Agent({
  maxHeaderSize: 128 * 1024,
  keepAlive: true
});

// 2. MySQL 连接
const pool = mysql.createPool({
  host: 'localhost',
  user: 'root',
  password: 'n3u3da!',
  database: 'finance_portfolio',
  port: 3306,
  connectionLimit: 10
});

// 3. 存储认证信息
let cookie = '';
let crumb = '';

// 4. 处理雅虎隐私政策同意
async function acceptPrivacyPolicy() {
  try {
    // 第一步：访问首页，获取初始 cookies
    const initialResponse = await axios.get('https://finance.yahoo.com', {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1'
      },
      withCredentials: true,
      httpsAgent: httpsAgent,
      timeout: 15000
    });

    // 提取初始 cookies
    const initialCookies = initialResponse.headers['set-cookie'];
    if (!initialCookies) throw new Error('访问首页未返回 cookies');
    
    // 查找隐私政策相关的 cookie（如 GUCE 或 CONSENT）
    const privacyCookies = initialCookies
      .map(c => c.split(';')[0])
      .filter(c => c.startsWith('GUCE=') || c.startsWith('CONSENT='));
    
    if (privacyCookies.length === 0) {
      console.log('⚠️ 未找到隐私相关 cookie，可能无需同意或结构已变');
      // 保存所有初始 cookies 继续尝试
      cookie = initialCookies.map(c => c.split(';')[0]).join('; ');
      return cookie;
    }
    
    // 保存隐私相关 cookies
    cookie = privacyCookies.join('; ');
    console.log('✅ 已获取隐私相关 cookies:', cookie);

    // 第二步：尝试直接访问数据接口，检查是否需要显式同意
    const testResponse = await axios.get('https://finance.yahoo.com/quote/AAPL', {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Cookie': cookie,
        'Referer': 'https://finance.yahoo.com'
      },
      withCredentials: true,
      httpsAgent: httpsAgent,
      timeout: 15000
    });

    // 检查响应内容是否包含实际数据（而非同意页面）
    const $ = cheerio.load(testResponse.data);
    const isConsentPage = $('form#privacy-form').length > 0;
    
    if (!isConsentPage) {
      console.log('✅ 无需显式同意隐私政策，继续获取 crumb');
      // 保存所有 cookies（可能包含额外的认证信息）
      const allCookies = testResponse.headers['set-cookie'];
      if (allCookies) {
        cookie += '; ' + allCookies.map(c => c.split(';')[0]).join('; ');
      }
      return cookie;
    }

    // 如果确实需要同意（实际很少走到这一步）
    console.log('⚠️ 仍需显式同意隐私政策（可能需要额外处理）');
    // 此处可添加更复杂的同意流程（如提交表单），但最新雅虎可能不需要
    
    return cookie;

  } catch (error) {
    console.error('处理隐私政策同意出错:', error.message);
    if (error.response) {
      console.error('响应状态:', error.response.status);
      console.error('响应内容长度:', error.response.data?.length || 0);
      // 打印部分响应内容用于调试
      console.error('响应内容前500字符:', error.response.data?.substring(0, 500));
    }
    throw error;
  }
}

// 5. 获取 crumb（改进提取逻辑）
async function getCrumbAfterConsent() {
  try {
    const response = await axios.get('https://finance.yahoo.com', {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
        'Cookie': cookie,
        'Referer': 'https://www.google.com/'
      },
      withCredentials: true,
      httpsAgent: httpsAgent,
      timeout: 15000
    });

    // 提取 crumb（使用多种可能的方法）
    const $ = cheerio.load(response.data);
    
    // 方法1：检查 window.YAHOO.context.crumb
    let crumbMatch = response.data.match(/"crumb":"([^"]+)"/);
    if (crumbMatch && crumbMatch[1]) {
      crumb = crumbMatch[1].replace(/\\u002F/g, '/');
      console.log(`✅ 通过方法1提取 crumb: ${crumb}`);
      return crumb;
    }
    
    // 方法2：检查 CrumbStore
    const crumbScript = $('script').filter((i, el) => {
      return $(el).html()?.includes('CrumbStore') || $(el).html()?.includes('crumb');
    }).first().html();
    
    if (crumbScript) {
      crumbMatch = crumbScript.match(/"crumb":"([^"]+)"/);
      if (crumbMatch && crumbMatch[1]) {
        crumb = crumbMatch[1].replace(/\\u002F/g, '/');
        console.log(`✅ 通过方法2提取 crumb: ${crumb}`);
        return crumb;
      }
    }
    
    // 方法3：检查 meta 标签或其他可能位置
    const metaCrumb = $('meta[name="crumb"]').attr('content');
    if (metaCrumb) {
      crumb = metaCrumb;
      console.log(`✅ 通过方法3提取 crumb: ${crumb}`);
      return crumb;
    }
    
    // 若所有方法都失败，尝试从响应中查找可能的 crumb 模式
    const fallbackCrumbMatch = response.data.match(/[A-Za-z0-9\/=]{11,}/);
    if (fallbackCrumbMatch && fallbackCrumbMatch[0]) {
      crumb = fallbackCrumbMatch[0];
      console.log(`⚠️ 通过模糊匹配提取 crumb: ${crumb}（可能不准确）`);
      return crumb;
    }
    
    throw new Error('无法从任何位置提取 crumb');

  } catch (error) {
    console.error('获取 crumb 失败:', error.message);
    throw error;
  }
}

// 6. 整合认证流程
async function getYahooAuth() {
  try {
    await acceptPrivacyPolicy();
    await getCrumbAfterConsent();
    console.log('🎉 认证完成，cookie 和 crumb 就绪');
  } catch (error) {
    console.error('雅虎认证流程失败:', error.message);
    throw error;
  }
}

// 7. 获取资产数据
async function fetchAssetData(ticker) {
  try {
    // 检查认证信息是否存在
    if (!cookie || !crumb) {
      console.log('🔄 认证信息缺失，重新获取');
      await getYahooAuth();
    }

    const url = `https://query1.finance.yahoo.com/v7/finance/quote?symbols=${ticker}&crumb=${crumb}`;
    const response = await axios.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
        'Cookie': cookie,
        'Accept': 'application/json',
        'Referer': 'https://finance.yahoo.com'
      },
      httpsAgent: httpsAgent,
      timeout: 15000
    });

    const quote = response.data.quoteResponse.result[0];
    if (!quote) throw new Error(`未找到 ${ticker} 数据`);

    return {
      ticker_symbol: quote.symbol,
      name: quote.shortName || quote.longName || ticker,
      asset_type: ticker.includes('-USD') ? 'crypto' : 'stock',
      current_price: quote.regularMarketPrice || 0,
      percent_change_today: quote.regularMarketChangePercent || 0,
      price_updated_at: new Date(),
      currency: quote.currency || 'USD'
    };

  } catch (error) {
    console.error(`获取 ${ticker} 数据失败:`, error.message);
    
    // 如果是认证相关错误，尝试重新认证
    if (error.response?.status === 401 || error.response?.status === 403) {
      console.log('🔄 认证失效，尝试重新获取');
      cookie = '';
      crumb = '';
      await getYahooAuth();
      return fetchAssetData(ticker); // 重试
    }
    
    throw error;
  }
}

// 8. 写入数据库
async function saveToAssetsTable(data) {
  if (!data) return;
  try {
    const query = `
      INSERT INTO Assets (
        ticker_symbol, name, asset_type, current_price, 
        percent_change_today, price_updated_at, currency
      )
      VALUES (?, ?, ?, ?, ?, ?, ?)
      ON DUPLICATE KEY UPDATE
        name = VALUES(name),
        current_price = VALUES(current_price),
        percent_change_today = VALUES(percent_change_today),
        price_updated_at = VALUES(price_updated_at),
        currency = VALUES(currency)
    `;
    const values = [
      data.ticker_symbol, data.name, data.asset_type,
      data.current_price, data.percent_change_today,
      data.price_updated_at, data.currency
    ];
    await pool.query(query, values);
    console.log(`✅ 保存 ${data.ticker_symbol} 成功`);
  } catch (error) {
    console.error(`写入 ${data.ticker_symbol} 失败:`, error.message);
  }
}

// 9. 批量同步
async function syncAssets(tickers) {
  try {
    await getYahooAuth();
    for (const ticker of tickers) {
      try {
        console.log(`🔄 开始同步 ${ticker}`);
        const data = await fetchAssetData(ticker);
        await saveToAssetsTable(data);
        console.log(`✅ ${ticker} 同步完成`);
      } catch (error) {
        console.error(`❌ ${ticker} 同步失败:`, error.message);
      }
      await new Promise(resolve => setTimeout(resolve, 5000)); // 间隔5秒
    }
    console.log('🎉 所有资产同步完成');
  } catch (error) {
    console.error('同步流程失败:', error.message);
  }
}

// 测试
const testTickers = ['AAPL', 'MSFT', 'GOOGL', 'BTC-USD', 'ETH-USD'];
syncAssets(testTickers);
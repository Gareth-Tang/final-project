

  // server.js
require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3000;

// 数据库连接池
const pool = new Pool({
  user: process.env.DB_USER || 'root',
  host: process.env.DB_HOST || 'localhost',
  database: process.env.DB_NAME || 'finance_portfolio',
  password: process.env.DB_PASSWORD || 'n3u3da!',
  port: process.env.DB_PORT || 3306,
});

// 中间件
app.use(cors()); // 允许跨域请求
app.use(express.json()); // 解析 JSON 请求体

// 测试数据库连接
app.get('/api/ping', async (req, res) => {
  try {
    const client = await pool.connect();
    await client.query('SELECT 1');
    client.release();
    res.status(200).json({ message: 'Database connection successful' });
  } catch (error) {
    res.status(500).json({ error: 'Database connection failed' });
  }
});

// 获取所有资产
app.get('/api/assets', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT * FROM Assets');
    res.status(200).json(rows);
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch assets' });
  }
});

// 根据 ticker 获取单个资产
app.get('/api/assets/:ticker', async (req, res) => {
  try {
    const { ticker } = req.params;
    const { rows } = await pool.query(
      'SELECT * FROM Assets WHERE ticker_symbol = $1', 
      [ticker]
    );
    
    if (rows.length === 0) {
      return res.status(404).json({ error: 'Asset not found' });
    }
    
    res.status(200).json(rows[0]);
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch asset' });
  }
});

// 启动服务器
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
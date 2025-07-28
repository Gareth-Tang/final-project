
-- 选择你创建的数据库（例如名为finance_portfolio）
USE finance_portfolio;

-- 全量字段表结构（PostgreSQL）
CREATE TABLE Assets (
    asset_id INT PRIMARY KEY AUTO_INCREMENT,
    ticker_symbol VARCHAR(20) UNIQUE, -- e.g., 'AAPL', 'MSFT'
    name VARCHAR(255) NOT NULL, -- e.g., 'Apple Inc.', 'Madrigal Electromotive'
    asset_type ENUM('stock', 'etf', 'option', 'mutual_fund', 'crypto') NOT NULL,
    current_price DECIMAL(20, 4),
    percent_change_today DECIMAL(8, 4), -- e.g., 2.53% 存为 2.53
    price_updated_at TIMESTAMP,
    currency CHAR(3) DEFAULT 'USD'
);


select * from assets
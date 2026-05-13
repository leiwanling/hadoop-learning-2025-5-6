-- mysql_schema.sql
-- 创建数据库
CREATE DATABASE IF NOT EXISTS realtime_db;
USE realtime_db;

-- 实时统计表（Spark Streaming 写入）
CREATE TABLE IF NOT EXISTS realtime_stats (
    window_start TIMESTAMP,    -- 窗口开始时间
    window_end TIMESTAMP,      -- 窗口结束时间
    pv BIGINT,                 -- 页面浏览量
    uv BIGINT,                 -- 独立访客数
    sales DOUBLE               -- 销售额
);

-- 离线日报表（Spark SQL 离线作业写入）
CREATE TABLE IF NOT EXISTS daily_reports (
    report_date DATE PRIMARY KEY,           -- 统计日期
    total_sales DOUBLE,                    -- 当日总销售额
    top1_product VARCHAR(50),              -- 热销商品第一名
    top1_sales DOUBLE,                     -- 第一名销售额
    top2_product VARCHAR(50),              -- 热销商品第二名
    top2_sales DOUBLE,                     -- 第二名销售额
    top3_product VARCHAR(50),              -- 热销商品第三名
    top3_sales DOUBLE,                     -- 第三名销售额
    click_uv BIGINT,                       -- 点击用户数
    cart_uv BIGINT,                        -- 加购用户数
    buy_uv BIGINT                          -- 购买用户数
);
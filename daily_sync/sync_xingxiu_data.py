"""
Module: daily_sync/sync_xingxiu_data.py
Purpose: 从 ai_export 接口抓取“昨天(Asia/Jakarta)”的数据，写入 xingxiu.xingxiu（含 AI 评分/总结）
Author: Jimmy 张杰铭
Updated: 2025-09-22
"""

import sys
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

import requests
import mysql.connector
from zoneinfo import ZoneInfo

# ===== 日志 =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ===== 目标接口 =====
API_BASE = "http://119.47.88.14:81"
API_PATH = "/admin/common/mechanical/ai_export"
API_TIMEOUT = 30  # seconds
API_HEADERS = {"Content-Type": "application/json; charset=utf-8"}

# ===== 取“雅加达昨天”或命令行参数 =====
def target_date_str() -> str:
    if len(sys.argv) > 1 and sys.argv[1].strip():
        return sys.argv[1].strip()
    now_jkt = datetime.now(ZoneInfo("Asia/Jakarta"))
    return (now_jkt - timedelta(days=1)).strftime("%Y-%m-%d")

DATE_STR = target_date_str()
logging.info(f"Target date (Asia/Jakarta yesterday or argv): {DATE_STR}")

# ===== DB 配置（仓库 Secrets / 环境变量）=====
# 记得把 DB_NAME 设置为 xingxiu
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "database": os.getenv("DB_NAME"),   # ← 在 Secrets 设置为 xingxiu
    "charset": "utf8mb4",
    "autocommit": False,
}

# ===== 目标表（表名固定：xingxiu）=====
TABLE = "xingxiu"

# ===== 建表 SQL：字段中文注释 =====
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS `{TABLE}` (
    `ID`                 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `DEVICE_NO`          VARCHAR(64)     NOT NULL COMMENT '工时通设备编号',
    `PROJECT_NAME`       VARCHAR(128)             COMMENT '项目名称',
    `MECHANICAL_NO`      VARCHAR(64)              COMMENT '园区机械编号',
    `DATE_STR`           DATE            NOT NULL COMMENT '日期',
    `RENT_TYPE`          VARCHAR(64)              COMMENT '租用方式',
    `TYPE_NAME`          VARCHAR(128)             COMMENT '机械品牌/类别/型号',
    `CAR_TYPE`           VARCHAR(64)              COMMENT '车辆/机械类别',
    `VALID_DURATION`     DECIMAL(10,2)            COMMENT '有效工时',
    `IDLING_DURATION`    DECIMAL(10,2)            COMMENT '怠速工时',
    `VALID_PERCENT`      DECIMAL(10,2)            COMMENT '工时有效比(%)',
    `DAY_OIL`            DECIMAL(12,2)_

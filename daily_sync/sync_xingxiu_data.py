"""
Script: sync_xingxiu_data.py
Purpose: 每天雅加达 01:30 抓“昨天”的接口数据，入库到表 `xingxiu`（首次自动建表，字段含中文注释）
Author: Jimmy 张杰铭
Updated: 2025-09-22
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

import requests
import mysql.connector
from zoneinfo import ZoneInfo

# 日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# ---------------- 接口配置（POST + Query Params） ----------------
API_URL = "http://119.47.88.14:81/admin/common/mechanical/ai_export"
API_HEADERS = {"Accept": "application/json"}

def jakarta_yesterday_str() -> str:
    """雅加达时区的昨天 YYYY-MM-DD"""
    now_jkt = datetime.now(ZoneInfo("Asia/Jakarta"))
    return (now_jkt - timedelta(days=1)).strftime("%Y-%m-%d")

# 允许命令行传日期；否则按“雅加达昨天”
DATE_STR = sys.argv[1].strip() if len(sys.argv) > 1 and sys.argv[1].strip() else jakarta_yesterday_str()

# 可选 key（Secrets 里设置 API_KEY）
API_PARAMS = {"dateStr": DATE_STR}
if os.getenv("API_KEY"):
    API_PARAMS["key"] = os.getenv("API_KEY")

# ---------------- 数据库配置（DB_NAME 设为你的库，如 xingxiu_db） ----------------
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "database": os.getenv("DB_NAME"),
    "charset": "utf8mb4",
    "autocommit": False,
}

TABLE_NAME = "xingxiu"

# 建表（中文注释）
TABLE_SCHEMA_SQL = f"""
CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
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
  `DAY_OIL`            DECIMAL(12,2)            COMMENT '当日油耗(L)',
  `DAY_REFUEL`         DECIMAL(12,2)            COMMENT '当日加油(L)',
  `DAY_MILEAGE`        DECIMAL(12,2)            COMMENT '当日里程(km)',
  `WORKHOUR_AVG_OIL`   DECIMAL(12,2)            COMMENT '工时场景平均油耗(L/h)',
  `TRANSPORT_AVG_OIL`  DECIMAL(12,2)            COMMENT '运输场景平均油耗(L/100km)',
  `COMPANY_ASSETS`     VARCHAR(128)             COMMENT '资产归属',
  `BELONG_LAND`        VARCHAR(128)             COMMENT '所属地块',
  `CREATE_TIME`        DATETIME                 COMMENT '进场/创建时间',
  `SCORE`              INT                      COMMENT 'AI 评分',
  `SUMMARY`            TEXT                     COMMENT 'AI 总结',
  PRIMARY KEY (`ID`),
  UNIQUE KEY `uniq_device_date` (`DEVICE_NO`,`DATE_STR`),
  KEY `idx_date` (`DATE_STR`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='星宿日报数据';
"""

# 插入（设备+日期去重）
INSERT_SQL = f"""
INSERT INTO `{TABLE_NAME}` (
  DEVICE_NO, PROJECT_NAME, DATE_STR, RENT_TYPE, MECHANICAL_NO, CREATE_TIME,
  TYPE_NAME, CAR_TYPE, VALID_DURATION, IDLING_DURATION, VALID_PERCENT,
  DAY_OIL, DAY_REFUEL, DAY_MILEAGE, WORKHOUR_AVG_OIL, TRANSPORT_AVG_OIL,
  COMPANY_ASSETS, BELONG_LAND, SCORE, SUMMARY
) VALUES (
  %(deviceNo)s, %(projectName)s, %(dateStr)s, %(rentType)s, %(mechanicalNo)s, %(createTime)s,
  %(typeName)s, %(carType)s, %(validDuration)s, %(idlingDuration)s, %(validPercent)s,
  %(dayOil)s, %(dayRefuel)s, %(dayMileage)s, %(workhourAvgOil)s, %(transportAvgOil)s,
  %(companyAssets)s, %(belongLand)s, %(score)s, %(summary)s
) ON DUPLICATE KEY UPDATE
  PROJECT_NAME=VALUES(PROJECT_NAME),
  RENT_TYPE=VALUES(RENT_TYPE),
  MECHANICAL_NO=VALUES(MECHANICAL_NO),
  CREATE_TIME=VALUES(CREATE_TIME),
  TYPE_NAME=VALUES(TYPE_NAME),
  CAR_TYPE=VALUES(CAR_TYPE),
  VALID_DURATION=VALUES(VALID_DURATION),
  IDLING_DURATION=VALUES(IDLING_DURATION),
  VALID_PERCENT=VALUES(VALID_PERCENT),
  DAY_OIL=VALUES(DAY_OIL),
  DAY_REFUEL=VALUES(DAY_REFUEL),
  DAY_MILEAGE=VALUES(DAY_MILEAGE),
  WORKHOUR_AVG_OIL=VALUES(WORKHOUR_AVG_OIL),
  TRANSPORT_AVG_OIL=VALUES(TRANSPORT_AVG_OIL),
  COMPANY_ASSETS=VALUES(COMPANY_ASSETS),
  BELONG_LAND=VALUES(BELONG_LAND),
  SCORE=VALUES(SCORE),
  SUMMARY=VALUES(SUMMARY);
"""

# ---- 小工具 ----
def normalize_date(s: str) -> str:
    try:
        y, m, d = [int(i) for i in str(s).split("-")]
        return datetime(y, m, d).strftime("%Y-%m-%d")
    except Exception:
        return str(s)

# ---- API ----
def fetch_api() -> List[Dict[str, Any]]:
    logging.info(f"POST {API_URL} params={API_PARAMS}")
    r = requests.post(API_URL, headers=API_HEADERS, params=API_PARAMS, timeout=20)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list):
        lst = data
    else:
        lst = data.get("dataList") or data.get("result") or data.get("data") or []
    if not isinstance(lst, list):
        raise RuntimeError("API dataList 不是列表")
    return lst

def transform(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for r in records:
        if r.get("dateStr"):
            r["dateStr"] = normalize_date(r["dateStr"])
    return records

# ---- DB ----
def get_conn():
    return mysql.connector.connect(**DB_CONFIG)

def ensure_table(cur):
    cur.execute(TABLE_SCHEMA_SQL)

def insert_records(records: List[Dict[str, Any]]):
    if not records:
        logging.info("无新增数据")
        return
    conn = get_conn()
    cur = conn.cursor()
    try:
        ensure_table(cur)
        cur.executemany(INSERT_SQL, records)
        conn.commit()
        logging.info(f"写入 {cur.rowcount} 条")
    finally:
        cur.close()
        conn.close()

# ---- 主流程 ----
def main():
    logging.info(f"开始同步（雅加达昨天={DATE_STR}）")
    rows = fetch_api()
    logging.info(f"API 返回 {len(rows)} 条")
    if rows:
        logging.info(f"样例 device/date：({rows[0].get('deviceNo')}, {rows[0].get('dateStr')})")
    insert_records(transform(rows))
    logging.info("完成")

if __name__ == "__main__":
    main()

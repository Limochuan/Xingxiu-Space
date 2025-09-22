"""
Script: sync_xingxiu_data.py
Purpose: 同步数据到数据库表 `xingxiu`
Author: Jimmy 张杰铭
Updated: 2025-09-22
"""

import sys
import os
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
import requests
import mysql.connector

# 日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# API 配置
API_URL = "http://119.47.88.14:81/admin/common/mechanical/ai_export"
API_HEADERS = {"Content-Type": "application/json"}

# 数据库配置
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "database": os.getenv("DB_NAME"),  # xingxiu_db
    "charset": "utf8mb4",
    "autocommit": False,
}

TABLE_NAME = "xingxiu"

# 建表 SQL
TABLE_SCHEMA_SQL = f"""
CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
    `ID` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `DEVICE_NO` VARCHAR(64) NOT NULL COMMENT '设备编号',
    `PROJECT_NAME` VARCHAR(128) COMMENT '项目名称',
    `MECHANICAL_NO` VARCHAR(64) COMMENT '机械编号',
    `TYPE_NAME` VARCHAR(128) COMMENT '机械品牌型号',
    `CAR_TYPE` VARCHAR(64) COMMENT '机械类别',
    `RENT_TYPE` VARCHAR(32) COMMENT '租用方式',
    `STANDARD_OIL` DECIMAL(12,2) COMMENT '标准油耗',
    `PRICE` DECIMAL(12,2) COMMENT '单价',
    `VALID_DURATION` DECIMAL(12,2) COMMENT '有效工时',
    `IDLING_DURATION` DECIMAL(12,2) COMMENT '怠速工时',
    `DAY_OIL` DECIMAL(12,2) COMMENT '当日油耗',
    `DAY_REFUEL` DECIMAL(12,2) COMMENT '当日加油',
    `DAY_MILEAGE` DECIMAL(12,2) COMMENT '当日里程',
    `VALID_PERCENT` DECIMAL(5,2) COMMENT '工时有效比',
    `WORKHOUR_AVG_OIL` DECIMAL(12,2) COMMENT '工时油耗',
    `TRANSPORT_AVG_OIL` DECIMAL(12,2) COMMENT '运输油耗',
    `DATE_STR` DATE NOT NULL COMMENT '日期',
    PRIMARY KEY (`ID`),
    UNIQUE KEY `uniq_device_date` (`DEVICE_NO`, `DATE_STR`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='星宿日报数据';
"""

# 插入 SQL
INSERT_SQL = f"""
INSERT INTO `{TABLE_NAME}` (
    DEVICE_NO, PROJECT_NAME, MECHANICAL_NO, TYPE_NAME, CAR_TYPE,
    RENT_TYPE, STANDARD_OIL, PRICE, VALID_DURATION, IDLING_DURATION,
    DAY_OIL, DAY_REFUEL, DAY_MILEAGE, VALID_PERCENT, WORKHOUR_AVG_OIL,
    TRANSPORT_AVG_OIL, DATE_STR
) VALUES (
    %(deviceNo)s, %(projectName)s, %(mechanicalNo)s, %(typeName)s, %(carType)s,
    %(rentType)s, %(standardOil)s, %(price)s, %(validDuration)s, %(idlingDuration)s,
    %(dayOil)s, %(dayRefuel)s, %(dayMileage)s, %(validPercent)s, %(workhourAvgOil)s,
    %(transportAvgOil)s, %(dateStr)s
) ON DUPLICATE KEY UPDATE
    PROJECT_NAME=VALUES(PROJECT_NAME),
    MECHANICAL_NO=VALUES(MECHANICAL_NO),
    TYPE_NAME=VALUES(TYPE_NAME),
    CAR_TYPE=VALUES(CAR_TYPE),
    RENT_TYPE=VALUES(RENT_TYPE),
    STANDARD_OIL=VALUES(STANDARD_OIL),
    PRICE=VALUES(PRICE),
    VALID_DURATION=VALUES(VALID_DURATION),
    IDLING_DURATION=VALUES(IDLING_DURATION),
    DAY_OIL=VALUES(DAY_OIL),
    DAY_REFUEL=VALUES(DAY_REFUEL),
    DAY_MILEAGE=VALUES(DAY_MILEAGE),
    VALID_PERCENT=VALUES(VALID_PERCENT),
    WORKHOUR_AVG_OIL=VALUES(WORKHOUR_AVG_OIL),
    TRANSPORT_AVG_OIL=VALUES(TRANSPORT_AVG_OIL);
"""

def fetch_api(date_str: str) -> List[Dict[str, Any]]:
    try:
        resp = requests.post(API_URL, headers=API_HEADERS, params={"dateStr": date_str}, timeout=30)
        resp.raise_for_status()
        return resp.json().get("result", [])
    except Exception as e:
        logging.error(f"API 请求失败: {e}")
        return []

def insert_records(records: List[Dict[str, Any]]):
    if not records:
        logging.info("无数据写入")
        return
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    try:
        cursor.execute(TABLE_SCHEMA_SQL)
        cursor.executemany(INSERT_SQL, records)
        conn.commit()
        logging.info(f"成功写入 {cursor.rowcount} 条记录")
    except mysql.connector.Error as err:
        conn.rollback()
        logging.error(f"写入失败: {err}")
    finally:
        cursor.close()
        conn.close()

def main():
    if len(sys.argv) == 3:  # 支持批量
        start_date = datetime.strptime(sys.argv[1], "%Y-%m-%d")
        end_date = datetime.strptime(sys.argv[2], "%Y-%m-%d")
        cur = start_date
        while cur <= end_date:
            d = cur.strftime("%Y-%m-%d")
            logging.info(f"拉取日期 {d}")
            rows = fetch_api(d)
            insert_records(rows)
            cur += timedelta(days=1)
    else:  # 默认昨天
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        logging.info(f"拉取昨天 {yesterday}")
        rows = fetch_api(yesterday)
        insert_records(rows)

if __name__ == "__main__":
    main()

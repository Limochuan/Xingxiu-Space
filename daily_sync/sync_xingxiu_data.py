"""
Module: daily_sync/sync_xingxiu_data.py
Purpose: 从 ai_export 接口抓取“昨天(Asia/Jakarta)”的数据，写入 xingxiu.xingxiu（含 AI 评分/总结）
Notes:
- 改为 POST + Query Params（与 Postman 一致），避免返回全 0 的问题
- 支持可选的 API_KEY（Secrets 设置 API_KEY 即可自动带上 ?key=xxx）
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
# Postman 使用的是 Query Params，所以这里不用 JSON 头
API_HEADERS = {"Accept": "application/json"}

# ===== 取“雅加达昨天”或命令行参数 =====
def target_date_str() -> str:
    if len(sys.argv) > 1 and sys.argv[1].strip():
        return sys.argv[1].strip()
    now_jkt = datetime.now(ZoneInfo("Asia/Jakarta"))
    return (now_jkt - timedelta(days=1)).strftime("%Y-%m-%d")

DATE_STR = target_date_str()
logging.info(f"Target date (Asia/Jakarta yesterday or argv): {DATE_STR}")

# ===== DB 配置（仓库 Secrets / 环境变量）=====
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "database": os.getenv("DB_NAME"),   # 建议设置为 xingxiu
    "charset": "utf8mb4",
    "autocommit": False,
}

# ===== 目标表 =====
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
    UNIQUE KEY `UK_DEVICE_DATE` (`DEVICE_NO`, `DATE_STR`),
    KEY `IDX_DATE` (`DATE_STR`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='工时通日报数据(含 AI 评分/总结)';
"""

# ===== UPSERT（设备+日期唯一）=====
UPSERT_SQL = f"""
INSERT INTO `{TABLE}` (
    DEVICE_NO, PROJECT_NAME, MECHANICAL_NO, DATE_STR, RENT_TYPE,
    TYPE_NAME, CAR_TYPE, VALID_DURATION, IDLING_DURATION, VALID_PERCENT,
    DAY_OIL, DAY_REFUEL, DAY_MILEAGE, WORKHOUR_AVG_OIL, TRANSPORT_AVG_OIL,
    COMPANY_ASSETS, BELONG_LAND, CREATE_TIME, SCORE, SUMMARY
) VALUES (
    %(DEVICE_NO)s, %(PROJECT_NAME)s, %(MECHANICAL_NO)s, %(DATE_STR)s, %(RENT_TYPE)s,
    %(TYPE_NAME)s, %(CAR_TYPE)s, %(VALID_DURATION)s, %(IDLING_DURATION)s, %(VALID_PERCENT)s,
    %(DAY_OIL)s, %(DAY_REFUEL)s, %(DAY_MILEAGE)s, %(WORKHOUR_AVG_OIL)s, %(TRANSPORT_AVG_OIL)s,
    %(COMPANY_ASSETS)s, %(BELONG_LAND)s, %(CREATE_TIME)s, %(SCORE)s, %(SUMMARY)s
) ON DUPLICATE KEY UPDATE
    PROJECT_NAME=VALUES(PROJECT_NAME),
    MECHANICAL_NO=VALUES(MECHANICAL_NO),
    RENT_TYPE=VALUES(RENT_TYPE),
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
    CREATE_TIME=VALUES(CREATE_TIME),
    SCORE=VALUES(SCORE),
    SUMMARY=VALUES(SUMMARY);
"""

# ===== 小工具 =====
def to_decimal(x: Any):
    if x is None or str(x).strip() == "" or str(x).lower() == "null":
        return None
    try:
        # 兼容 "1,234.56" 或 "1.234,56" 这类格式
        s = str(x).replace(",", "")
        return float(s)
    except Exception:
        try:
            return float(x)
        except Exception:
            return None

def to_int(x: Any):
    try:
        return int(float(str(x).replace(",", "")))
    except Exception:
        return None

def to_date(x: Any):
    if not x:
        return None
    s = str(x).strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date().strftime("%Y-%m-%d")
    except Exception:
        return None

def to_datetime(x: Any):
    if not x:
        return None
    s = str(x).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def extract_list(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("dataList", "result", "data"):
            v = payload.get(key)
            if isinstance(v, list):
                return v
    return []

# ===== 接口请求（关键修复：POST + Query Params；支持 key）=====
def fetch_api(date_str: str) -> List[Dict[str, Any]]:
    url = f"{API_BASE}{API_PATH}"
    params = {"dateStr": date_str}
    api_key = os.getenv("API_KEY") or os.getenv("API_TOKEN") or os.getenv("KEY")
    if api_key:
        params["key"] = api_key
    logging.info(f"POST {url} params={params}")
    r = requests.post(url, headers=API_HEADERS, params=params, timeout=API_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    rows = extract_list(data)
    # 如果外层不是 dataList/result/data，而是直接 list
    if not rows and isinstance(data, list):
        rows = data
    logging.info(f"rows: {len(rows)}")
    if rows[:1]:
        logging.info(f"sample: {json.dumps(rows[0], ensure_ascii=False)}")
    return rows

def map_row(r: Dict[str, Any], date_str: str) -> Dict[str, Any]:
    return {
        "DEVICE_NO":         (r.get("deviceNo") or r.get("DEVICE_NO") or "")[:64],
        "PROJECT_NAME":      r.get("projectName"),
        "MECHANICAL_NO":     r.get("mechanicalNo"),
        "DATE_STR":          to_date(r.get("dateStr")) or date_str,
        "RENT_TYPE":         str(r.get("rentType")) if r.get("rentType") is not None else None,
        "TYPE_NAME":         r.get("typeName"),
        "CAR_TYPE":          r.get("carType") or r.get("catType"),
        "VALID_DURATION":    to_decimal(r.get("validDuration")),
        "IDLING_DURATION":   to_decimal(r.get("idlingDuration")),
        "VALID_PERCENT":     to_decimal(r.get("validPercent")),
        "DAY_OIL":           to_decimal(r.get("dayOil")),
        "DAY_REFUEL":        to_decimal(r.get("dayRefuel")),
        "DAY_MILEAGE":       to_decimal(r.get("dayMileage")),
        "WORKHOUR_AVG_OIL":  to_decimal(r.get("workhourAvgOil")),
        "TRANSPORT_AVG_OIL": to_decimal(r.get("transportAvgOil")),
        "COMPANY_ASSETS":    r.get("companyAssets"),
        "BELONG_LAND":       r.get("belongLand"),
        "CREATE_TIME":       to_datetime(r.get("createTime")),
        "SCORE":             to_int(r.get("score")),
        "SUMMARY":           r.get("summary"),
    }

def get_conn():
    return mysql.connector.connect(**DB_CONFIG)

def ensure_table(cur):
    cur.execute(CREATE_TABLE_SQL)

def upsert(rows: List[Dict[str, Any]]):
    if not rows:
        logging.info("no rows to write")
        return
    conn = get_conn()
    cur = conn.cursor()
    try:
        ensure_table(cur)
        cur.executemany(UPSERT_SQL, rows)
        conn.commit()
        logging.info(f"upsert OK: {cur.rowcount} rows")
    except mysql.connector.Error as e:
        conn.rollback()
        logging.error(f"DB write failed: {e}")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()

def main():
    logging.info(f"Start sync -> table={TABLE} date={DATE_STR}")
    raw = fetch_api(DATE_STR)
    mapped = [map_row(r, DATE_STR) for r in raw if (r.get("deviceNo") or r.get("DEVICE_NO"))]
    mapped = [m for m in mapped if m.get("DATE_STR")]
    logging.info(f"ready to write: {len(mapped)}")
    upsert(mapped)
    logging.info("Done")

if __name__ == "__main__":
    main()

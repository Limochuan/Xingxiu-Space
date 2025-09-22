# Xingxiu Space (星宿)

此仓库包含多个功能模块。  
- `daily_sync/`：每天 01:30 (Asia/Jakarta) 从 ai_export 接口抓“昨天”的数据，写入 MySQL 表 `xingxiu.xingxiu`。

## 部署
1. 仓库 Settings → Secrets and variables → Actions，添加：
   - `DB_HOST` / `DB_PORT` / `DB_USER` / `DB_PASS` / `DB_NAME`（设为 `xingxiu`）
2. Actions → 选择 **Xingxiu Daily Sync**，可手动 **Run workflow** 验证。

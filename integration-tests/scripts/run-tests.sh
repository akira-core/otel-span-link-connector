#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_DIR="$(dirname "$SCRIPT_DIR")"
# 專案根目錄（integration-tests 的上一層）
cd "$COMPOSE_DIR/.."
# 啟動所有服務（testapp 會一起啟動並在送完 trace 後退出）
docker compose -f integration-tests/docker-compose.yaml up -d --build
# 等待 testapp 完成（內含約 15s 等待 collector 就緒）
docker compose -f integration-tests/docker-compose.yaml wait testapp 2>/dev/null || true
# 再等 metrics flush（collector 每 5 秒 flush）
sleep 20
# 執行 Go 測試驗證
docker compose -f integration-tests/docker-compose.yaml run --rm verify
# 清理
docker compose -f integration-tests/docker-compose.yaml down -v
echo "ALL TESTS PASSED"

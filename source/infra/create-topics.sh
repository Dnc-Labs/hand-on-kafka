#!/usr/bin/env bash
# =============================================================================
#  TẠO TOPIC cho hệ Banking/Wallet — "Topics as Code"
# =============================================================================
#  Chạy:  bash source/infra/create-topics.sh
#  (Yêu cầu cluster đang chạy: docker compose up -d)
#
#  Số partition & RF theo thiết kế ở Bài 1.1 (Q&A "số partition nên đặt bao nhiêu").
#  RF=3 + min.insync.replicas=2 = khẩu quyết độ bền Bài 1.2.
#  cleanup.policy=delete + retention dài = event/facts (Bài 1.6).
# =============================================================================
set -euo pipefail

BROKER="localhost:9092"          # listener INTERNAL bên trong container kafka1
EXEC="docker exec kafka1 kafka-topics --bootstrap-server ${BROKER}"

# Hàm tạo 1 topic (idempotent nhờ --if-not-exists)
create() {
  local name=$1 partitions=$2 retention_ms=$3
  echo "→ Tạo topic ${name} (partitions=${partitions}, RF=3, retention=${retention_ms}ms)"
  $EXEC --create --if-not-exists \
    --topic "${name}" \
    --partitions "${partitions}" \
    --replication-factor 3 \
    --config min.insync.replicas=2 \
    --config cleanup.policy=delete \
    --config retention.ms="${retention_ms}"
}

# retention 7 ngày = 604800000 ms (mặc định học tập; topic audit/ledger thật sẽ để dài hơn nhiều)
R7D=604800000

# --- Topic theo luồng Transfer Money (Bài 0.5) ---
create "banking.transfer.requested"  6 "$R7D"   # hot path đầu vào; key = transferId
create "banking.account.debited"     6 "$R7D"   # hot path; key = accountId (giữ thứ tự per-ví)
create "banking.account.credited"    6 "$R7D"   # hot path; key = accountId
create "banking.fraud.checked"       3 "$R7D"   # tải vừa
create "banking.transfer.completed"  3 "$R7D"   # chỉ Notification nghe, tải thấp

echo ""
echo "✅ Hoàn tất. Danh sách topic:"
docker exec kafka1 kafka-topics --bootstrap-server "${BROKER}" --list | grep '^banking'

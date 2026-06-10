# Lộ trình học Kafka & Microservices (Banking/Wallet)

> Mục tiêu: từ Java developer 3 năm → senior, nắm vững **Kafka** và **event-driven microservices**
> qua một hệ thống Banking/Wallet production-grade.

Ký hiệu trạng thái: ⬜ chưa học · 🟡 đang học · ✅ xong

---

## MODULE 0 — Nền tảng Microservices (lý thuyết)
- ✅ 0.1 Monolith vs Microservices: định nghĩa, động cơ, trade-offs
- ✅ 0.2 Giao tiếp giữa service: synchronous (REST/gRPC) vs asynchronous (messaging)
- ✅ 0.3 Vì sao cần message broker — Kafka đứng ở đâu trong bức tranh
- ✅ 0.4 Bài toán nhất quán dữ liệu phân tán: distributed transaction, CAP, eventual consistency
- ✅ 0.5 Thiết kế domain Banking/Wallet: bounded context, các service & event chính

## MODULE 1 — Kafka Core Concepts (lý thuyết sâu)
- ✅ 1.1 Kiến trúc tổng quan: broker, controller (KRaft), topic, partition, offset, segment
  - Hỏi đáp: số partition nên đặt bao nhiêu? · cần thứ tự tuyệt đối toàn topic thì làm thế nào?
- ✅ 1.2 Replication: leader/follower, ISR, `acks`, `min.insync.replicas`, độ bền dữ liệu
  - Hỏi đáp: RF=3 có tốn gấp 3 đĩa? · so với DB tự replicate khác gì?
- ✅ 1.3 Producer internals: partitioner, batching, `linger.ms`, compression, idempotent producer
  - Hỏi đáp: PID có sống sót khi producer restart không?
- ✅ 1.4 Consumer internals: consumer group, rebalance, offset commit, assignment strategy
  - Hỏi đáp: replay từ thời điểm quá khứ làm thế nào? · quan sát offset/lag bằng tool nào (Kafka UI + Prometheus/Grafana)?
- ✅ 1.5 Delivery semantics: at-most-once / at-least-once / exactly-once, transactions
  - Hỏi đáp: Outbox vs Kafka transactions khác nhau thế nào, sao không dùng transactions cho DB?
- ✅ 1.6 Log retention & compaction, tiered storage (stream–table duality)

## MODULE 2 — Dựng hạ tầng (thực hành đầu tiên)
- ⬜ 2.1 Docker Compose: cluster 3 broker KRaft + Schema Registry + Kafka UI
- ⬜ 2.2 Kafka CLI: tạo topic, produce/consume, mô tả cluster
- ⬜ 2.3 Khởi tạo project: multi-module Maven + Spring Boot skeleton

## MODULE 3 — Schema & Serialization
- ⬜ 3.1 Avro & Schema Registry: subject, schema id, wire format
- ⬜ 3.2 Compatibility & schema evolution: backward / forward / full
- ⬜ 3.3 Sinh code từ Avro schema (Maven plugin), quản lý schema trong repo

## MODULE 4 — Producer thực hành (production-grade)
- ⬜ 4.1 Spring Kafka producer: cấu hình prod-grade (idempotence, `acks=all`)
- ⬜ 4.2 Gửi message, key strategy, partition theo aggregate id
- ⬜ 4.3 Xử lý lỗi gửi, callback, retry, timeout

## MODULE 5 — Consumer thực hành (production-grade)
- ⬜ 5.1 Spring Kafka consumer: listener container, manual ack, concurrency
- ⬜ 5.2 Error handling: `DefaultErrorHandler`, retry/backoff
- ⬜ 5.3 Dead Letter Topic (DLT) & `DeadLetterPublishingRecoverer`
- ⬜ 5.4 Idempotent consumer (chống xử lý trùng)

## MODULE 6 — Patterns nhất quán dữ liệu (trái tim của khóa học)
- ⬜ 6.1 Transactional Outbox pattern (atomic DB + event)
- ⬜ 6.2 Saga pattern — Choreography vs Orchestration
- ⬜ 6.3 Triển khai Saga cho luồng chuyển tiền (transfer money)
- ⬜ 6.4 CQRS & materialized view cho Ledger / số dư
- ⬜ 6.5 Exactly-once semantics thực chiến: read-process-write transactions

## MODULE 7 — Stream processing (Kafka Streams)
- ⬜ 7.1 Khái niệm: KStream, KTable, state store, windowing
- ⬜ 7.2 Fraud detection real-time trên luồng giao dịch
- ⬜ 7.3 Tổng hợp Ledger / số dư bằng Kafka Streams

## MODULE 8 — Production concerns
- ⬜ 8.1 Observability: metrics (Micrometer), distributed tracing, lag monitoring
- ⬜ 8.2 Performance tuning: throughput vs latency, partition sizing
- ⬜ 8.3 Security: TLS, SASL, ACL
- ⬜ 8.4 Vận hành: rolling restart, reassignment, disaster recovery, multi-DC

---

## Nguyên tắc xuyên suốt
- Mỗi bài: **lý thuyết từng mục nhỏ → hỏi đáp → docs → thực hành**.
- Mọi cấu hình hướng **production**; tiện ích dev/local sẽ được chỉ rõ và hỏi ý kiến.
- Docs mỗi buổi giữ **nguyên văn** lý thuyết + câu hỏi + câu trả lời.

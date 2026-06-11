# Hạ tầng Kafka local — Banking/Wallet

Cluster Kafka cho việc học: **3 broker (KRaft) + Schema Registry + Kafka UI**, dựng bằng Docker Compose.

> ⚠️ **Local/học tập.** Cấu hình độ bền (RF=3, min ISR=2, no auto-create) là prod-grade; còn PLAINTEXT, 3 broker/1 máy, và bản thân Docker Compose là rút gọn cho local. Xem chú thích `[LOCAL]` trong `docker-compose.yml`.

## Cổng (port) truy cập từ host

| Dịch vụ | Địa chỉ | Dùng để |
|---|---|---|
| Kafka broker 1 | `localhost:19092` | bootstrap server cho app (Spring) |
| Kafka broker 2 | `localhost:19093` | bootstrap server |
| Kafka broker 3 | `localhost:19094` | bootstrap server |
| Schema Registry | http://localhost:8081 | quản Avro schema |
| Kafka UI | http://localhost:8080 | nhìn trực quan topic/partition/offset/lag |

App chạy trên host nên dùng: `bootstrap.servers = localhost:19092,localhost:19093,localhost:19094`

## Lệnh thường dùng

```bash
# Khởi động cluster (nền)
docker compose up -d

# Xem trạng thái + healthcheck
docker compose ps

# Xem log một broker
docker compose logs -f kafka1

# Dừng (giữ dữ liệu)
docker compose down

# Dừng + XÓA SẠCH dữ liệu (reset cluster về trắng)
docker compose down -v
```

## Kiểm tra nhanh sau khi up

```bash
# Liệt kê broker đang sống (chạy bên trong container kafka1)
docker exec -it kafka1 kafka-broker-api-versions --bootstrap-server localhost:9092 | head

# Hoặc mở trình duyệt: http://localhost:8080 (Kafka UI) → thấy cluster "banking-local" với 3 broker
```

## Ghi chú khi lên Production (để đối chiếu)
- **Bảo mật**: thay PLAINTEXT bằng TLS + SASL (SCRAM/mTLS) + ACL — Module 8.3.
- **Triển khai**: Kubernetes + Strimzi operator, hoặc managed (Confluent Cloud / MSK).
- **Tách biệt lỗi**: mỗi broker một node/máy, trải trên nhiều availability zone.
- **Lưu trữ**: volume bền được định cỡ, giám sát; cân nhắc Tiered Storage (Bài 1.2).
- **Quan sát**: Prometheus + Grafana + kafka-exporter + alert lag — Module 8.1.

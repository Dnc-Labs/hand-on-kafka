# Learn Kafka & Microservices — Banking/Wallet System

Lộ trình học **Apache Kafka** và **Microservices** thông qua việc xây dựng một hệ thống
Banking/Wallet thực tế, theo phong cách production-grade.

## Cấu hình đã thống nhất

| Hạng mục            | Lựa chọn                                                        |
|---------------------|-----------------------------------------------------------------|
| Domain              | Banking / Wallet (Account, Transaction, Ledger, Fraud-check)    |
| Hạ tầng Kafka       | Docker Compose — cluster 3 broker + Schema Registry + Kafka UI   |
| Serialization       | Avro + Schema Registry                                           |
| Nhịp độ             | Lý thuyết kỹ → rồi mới code                                      |
| Ngôn ngữ giảng dạy  | Tiếng Việt (giữ keyword tiếng Anh)                              |
| Java / Build        | Java 21 (LTS) / Maven 3.9                                        |

## Cấu trúc thư mục

```
learn-kafka/
├── docs/      # Tài liệu mỗi buổi học (lý thuyết + câu hỏi + trả lời, giữ nguyên văn)
└── source/    # Toàn bộ source code dự án
```

## Cách chúng ta làm việc

1. **Học lý thuyết** theo từng mục nhỏ trong mỗi bài của roadmap.
2. Bạn **đặt câu hỏi** ở mỗi phần (nếu có) — tôi giải đáp.
3. **Kết thúc bài** → tôi tạo docs đầy đủ (lý thuyết + câu hỏi + trả lời, nguyên văn).
4. **Thực hành**: tôi tạo skeleton, bạn code, tôi review.
5. Mọi hướng dẫn hướng tới **production**. Chỗ nào là tiện ích dev/local, tôi sẽ chỉ rõ và hỏi ý bạn.

Xem lộ trình chi tiết tại [`docs/00-roadmap.md`](docs/00-roadmap.md).

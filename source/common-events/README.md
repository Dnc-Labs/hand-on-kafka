# common-events

Module chứa **contract** (Avro schema) dùng chung giữa các service — chính là "hợp đồng"
giao tiếp event mà ta nói ở Bài 0.2.

- Hiện tại: chỉ có khung module + dependency Avro core.
- **Module 3** sẽ thêm: file schema `.avsc` trong `src/main/avro/`, và `avro-maven-plugin`
  để sinh code Java từ schema lúc build. Các service phụ thuộc module này để dùng chung
  cùng một định nghĩa event (TransferRequested, MoneyDebited, ...).

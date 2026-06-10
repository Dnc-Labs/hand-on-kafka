# MODULE 0 — Nền tảng Microservices

> Tài liệu này ghi lại **nguyên văn** nội dung buổi học: lý thuyết + câu hỏi của học viên + câu trả lời.
> Mỗi mục được bổ sung khi kết thúc; roadmap (`00-roadmap.md`) được cập nhật trạng thái tương ứng.

Mục lục:
- [Bài 0.1 — Monolith vs Microservices](#bài-01--monolith-vs-microservices)
- [Bài 0.2 — Synchronous vs Asynchronous](#bài-02--giao-tiếp-giữa-service--synchronous-vs-asynchronous)
- [Bài 0.3 — Vì sao cần Message Broker, Kafka đứng ở đâu](#bài-03--vì-sao-cần-message-broker--và-kafka-đứng-ở-đâu)
- [Bài 0.4 — Bài toán nhất quán dữ liệu phân tán](#bài-04--bài-toán-nhất-quán-dữ-liệu-phân-tán)
- [Bài 0.5 — Thiết kế domain Banking/Wallet](#bài-05--thiết-kế-domain-bankingwallet)

---

# Bài 0.1 — Monolith vs Microservices

## 1. Monolith là gì?

**Monolith** (kiến trúc *nguyên khối*) là ứng dụng mà **toàn bộ chức năng nghiệp vụ được đóng gói và deploy như một đơn vị duy nhất**. Trong thế giới Java, đó thường là **một file `.jar`/`.war`**, chạy trong một process, kết nối tới **một database dùng chung**.

Ví dụ với hệ Banking của chúng ta, nếu làm monolith:

```
┌─────────────────────────────────────┐
│         banking-app.jar             │
│  ┌────────┐ ┌──────────┐ ┌────────┐ │
│  │Account │ │Transaction│ │ Fraud  │ │   ← tất cả module
│  │ module │ │  module   │ │ module │ │      trong 1 process
│  └────────┘ └──────────┘ └────────┘ │
└──────────────────┬──────────────────┘
                   │
            ┌──────▼───────┐
            │  ONE Database │              ← 1 DB dùng chung
            └──────────────┘
```

Các module gọi nhau bằng **method call trong cùng JVM** — nhanh, đơn giản, type-safe.

> ⚠️ Lưu ý quan trọng: Monolith **không phải** là "code tệ". Một monolith được tổ chức tốt (gọi là *modular monolith*) là điểm khởi đầu hoàn toàn hợp lý. Đa số dự án **nên bắt đầu từ monolith**.

## 2. Microservices là gì?

**Microservices** là kiến trúc chia hệ thống thành **nhiều service nhỏ, độc lập**, mỗi service:

- **Sở hữu một năng lực nghiệp vụ riêng** (business capability) — ví dụ: *Account Service*, *Transaction Service*, *Fraud Service*.
- **Có database riêng** — không service nào được đụng thẳng vào DB của service khác (nguyên tắc *database per service*).
- **Deploy độc lập** — sửa và release Fraud Service không cần build lại Account Service.
- **Giao tiếp qua mạng** — REST/gRPC (đồng bộ) hoặc message/event qua Kafka (bất đồng bộ).

```
┌─────────────┐   ┌──────────────┐   ┌────────────┐
│   Account   │   │ Transaction  │   │   Fraud    │
│   Service   │   │   Service    │   │  Service   │   ← process riêng,
│  ┌───────┐  │   │  ┌────────┐  │   │ ┌───────┐  │      deploy riêng
│  │ DB    │  │   │  │  DB    │  │   │ │  DB   │  │
│  └───────┘  │   │  └────────┘  │   │ └───────┘  │   ← DB riêng
└──────┬──────┘   └──────┬───────┘   └─────┬──────┘
       └──────────────── Kafka ────────────┘            ← giao tiếp qua network
```

## 3. Tại sao chia nhỏ? (động cơ thật sự)

Người ta KHÔNG chuyển sang microservices vì "nó hiện đại". Động cơ thật là:

| Động cơ | Giải thích |
|---|---|
| **Independent deployment** | Team Fraud deploy 10 lần/ngày mà không ảnh hưởng team Account. Giảm rủi ro release. |
| **Independent scaling** | Fraud-check tốn CPU → scale riêng 20 instance; Account chỉ cần 3. Monolith phải scale **cả khối**. |
| **Team autonomy** | Mỗi team sở hữu trọn vẹn một service (Conway's Law). Hệ thống lớn, nhiều team mới cần điều này. |
| **Fault isolation** | Fraud Service chết không kéo sập việc xem số dư. (Nhưng phải thiết kế đúng mới đạt được.) |
| **Technology diversity** | Service này Java, service kia Go/Python. (Lợi ích này thường bị thổi phồng.) |

## 4. Cái giá phải trả (đây là phần senior cần thuộc)

Microservices **không miễn phí**. Bạn đánh đổi *độ phức tạp trong code* lấy *độ phức tạp vận hành & phân tán*:

| Cái giá | Vì sao đau |
|---|---|
| **Distributed system complexity** | Network có thể chậm, mất gói, gọi 2 lần. Mọi lời gọi đều **có thể thất bại**. |
| **Data consistency** | Không còn ACID transaction xuyên service. Đây là lý do **chính** ta cần Kafka + Saga/Outbox. |
| **Operational overhead** | Phải có CI/CD, monitoring, tracing, service discovery cho hàng chục service. |
| **Testing khó hơn** | Test một luồng chạy qua 4 service phức tạp hơn nhiều so với gọi method. |
| **Debug khó hơn** | Một request đi qua 5 service — phải có *distributed tracing* mới lần ra lỗi. |

> 💡 Câu nói kinh điển của senior: *"You shouldn't start with microservices. You earn the right to microservices by first hitting the limits of a monolith."*

## 5. Liên hệ trực tiếp tới khóa học của chúng ta

Trong domain **Banking/Wallet**, khi tách thành Account / Transaction / Ledger / Fraud service, ta lập tức gặp bài toán kinh điển:

> *Chuyển 1.000.000đ từ ví A sang ví B. Trừ tiền ví A (Account Service) và cộng tiền ví B nằm ở DB khác. Nếu trừ xong, máy sập trước khi cộng thì sao? Không thể dùng một `@Transactional` ôm cả hai vì chúng ở hai DB/hai service khác nhau.*

**Chính bài toán này** là lý do toàn bộ phần lõi của khóa (Module 6: Outbox, Saga, Exactly-once) tồn tại — và là lý do **Kafka** có mặt. Microservices *tạo ra* vấn đề nhất quán dữ liệu; Kafka + các pattern là cách *giải* nó.

---

📌 **Tóm tắt Bài 0.1:** Monolith = một khối, một DB, gọi nhau bằng method. Microservices = nhiều service độc lập, DB riêng, gọi qua mạng. Đổi sự đơn giản lấy khả năng deploy/scale độc lập — nhưng trả giá bằng độ phức tạp phân tán, mà nặng nhất là **nhất quán dữ liệu**.

**Hỏi đáp 0.1:** _(Học viên không có câu hỏi.)_

---

# Bài 0.2 — Giao tiếp giữa service — Synchronous vs Asynchronous

Khi đã tách thành nhiều service, câu hỏi sống còn là: **chúng nói chuyện với nhau bằng cách nào?** Có hai trường phái lớn, và hiểu rõ trade-off giữa chúng là kiến thức nền để hiểu *vì sao Kafka tồn tại*.

## 1. Synchronous communication (giao tiếp đồng bộ)

**Định nghĩa:** Service A gọi Service B và **chờ (block) cho tới khi có response** mới làm tiếp. Đây là mô hình **request/response**.

Công nghệ điển hình: **REST (HTTP)**, **gRPC**.

```
Transaction Service          Account Service
      │                            │
      │  POST /accounts/A/debit    │
      │ ──────────────────────────▶│   (A gọi và ĐỨNG CHỜ)
      │                            │  xử lý trừ tiền...
      │  200 OK { balance: ... }   │
      │ ◀──────────────────────────│
      │                            │
   (chờ xong mới đi tiếp)
```

**Đặc điểm cốt lõi:**
- A **biết** B là ai, B ở đâu (URL/endpoint) → **temporal coupling** (ràng buộc thời gian: B phải sống và rảnh *ngay lúc đó*).
- A nhận kết quả **ngay lập tức** → dễ lập trình, dễ suy luận, dễ debug.
- A **phụ thuộc vào sự sẵn sàng của B**: nếu B chậm, A chậm theo. Nếu B chết, A lỗi.

## 2. Asynchronous communication (giao tiếp bất đồng bộ)

**Định nghĩa:** Service A gửi một **message/event** đi rồi **làm tiếp ngay, không chờ** kết quả. Một bên thứ ba — **message broker (Kafka)** — nhận, lưu trữ, và chuyển phát message đó cho (các) service quan tâm.

```
Transaction Service        Kafka (broker)        Account Service
      │                         │                      │
      │  publish "MoneyDebited" │                      │
      │ ───────────────────────▶│  (lưu vào log)       │
      │  (đi tiếp ngay)         │                      │
      │                         │ ───── deliver ──────▶│
      │                         │                      │ xử lý
```

**Đặc điểm cốt lõi:**
- A **không biết** ai sẽ xử lý message, cũng không cần biết → **decoupling** (tách rời).
- A **không chờ** → trả lời người dùng nhanh, throughput cao.
- B **không cần sống đúng lúc A gửi**: Kafka giữ message lại, B chết rồi sống lại vẫn đọc được → phá vỡ *temporal coupling*.

## 3. Hai phong cách message bất đồng bộ (rất hay bị nhầm)

Đây là chỗ nhiều người mơ hồ. Có hai *kiểu* dùng message khác nhau về bản chất:

| | **Command/Message (queue)** | **Event (event-driven)** |
|---|---|---|
| Ý nghĩa | "Hãy làm việc X cho tôi" | "Việc X đã xảy ra rồi" |
| Hướng | Người gửi **biết** muốn ai đó làm gì | Người gửi **chỉ thông báo**, không quan tâm ai nghe |
| Ví dụ | `SendEmailCommand` | `MoneyTransferred` (event) |
| Coupling | Còn coupling về ý định | Decoupling mạnh nhất |
| Số người nhận | Thường 1 | 0, 1, hoặc nhiều (ai quan tâm thì nghe) |

Kafka làm tốt **cả hai**, nhưng sức mạnh thật sự của nó nằm ở mô hình **event-driven**: một event `MoneyTransferred` được Account ghi ra, thì *Ledger Service* nghe để cập nhật sổ cái, *Fraud Service* nghe để phân tích, *Notification Service* nghe để gửi SMS — **mà Account không hề biết tới sự tồn tại của 3 service kia**. Thêm service thứ 4 sau này? Chỉ cần nó subscribe, **không sửa code Account**.

## 4. So sánh thẳng — đây là bảng senior cần thuộc

| Tiêu chí | Synchronous (REST/gRPC) | Asynchronous (Kafka) |
|---|---|---|
| **Coupling** | Chặt (temporal + location) | Lỏng |
| **Độ trễ cảm nhận** | Chờ toàn bộ chuỗi | Trả lời ngay, xử lý nền |
| **Khả năng chịu lỗi** | B chết → A lỗi ngay | B chết → message vẫn nằm chờ trong Kafka |
| **Backpressure / tải đột biến** | Dồn tải trực tiếp lên B, dễ sập dây chuyền | Kafka làm **buffer**, B xử lý theo nhịp của nó |
| **Suy luận / debug** | Dễ (gọi → nhận) | Khó hơn (luồng rời rạc, cần tracing) |
| **Tính nhất quán** | Dễ trả lỗi tức thì cho client | *Eventual consistency* — phải chấp nhận độ trễ |
| **Hợp với** | Truy vấn cần kết quả ngay (xem số dư) | Lan truyền thay đổi trạng thái, xử lý nền, fan-out |

## 5. Vấn đề chí mạng của Synchronous: Cascading Failure

Đây là lý do lớn khiến hệ microservices "thuần REST" hay sập dây chuyền:

```
User ─▶ Transaction ─▶ Account ─▶ Fraud ─▶ Ledger
                                    │
                            (Fraud chậm 5s)
```

Nếu Fraud chậm, Account chờ → Transaction chờ → các thread bị giữ → hết thread pool → **Transaction cũng sập** dù bản thân nó khỏe. Một service yếu kéo sập cả chuỗi. Người ta phải vá bằng *timeout, retry, circuit breaker* — phức tạp.

Với async qua Kafka: Transaction ghi event xong là xong việc của nó. Fraud chậm thì cứ chậm, message **nằm chờ trong Kafka**, không ai bị block. Đây là tính chất **temporal decoupling** + **buffering** — một trong những lý do mạnh nhất để dùng Kafka.

## 6. Sự thật của senior: KHÔNG phải chọn một

Hệ thống thật **dùng cả hai**, đúng chỗ:

- **Synchronous** cho *query cần kết quả ngay*: "xem số dư ví A" → REST, vì user cần thấy số ngay.
- **Asynchronous** cho *lan truyền thay đổi trạng thái & xử lý nền*: "tiền vừa được chuyển" → Kafka event, để Ledger/Fraud/Notification xử lý.

Trong dự án Banking của chúng ta, ta sẽ dùng **REST cho các lệnh từ client** (gọi vào hệ thống) và **Kafka cho giao tiếp event giữa các service** (lan truyền bên trong). Một nguyên tắc thiết kế tốt: **lệnh đi vào bằng sync, sự kiện lan ra bằng async**.

---

📌 **Tóm tắt Bài 0.2:** Synchronous (REST/gRPC) = gọi và chờ, đơn giản nhưng coupling chặt và dễ sập dây chuyền. Asynchronous (Kafka) = gửi event và đi tiếp, tách rời mạnh, Kafka làm buffer chịu lỗi và chịu tải — đổi lại là *eventual consistency* và khó debug hơn. Hệ thật dùng cả hai: **sync cho query, async cho lan truyền sự kiện**.

**Hỏi đáp 0.2:** _(Học viên không có câu hỏi.)_

---

# Bài 0.3 — Vì sao cần Message Broker — và Kafka đứng ở đâu

Ở 0.2 ta nói "gửi qua một broker". Nhưng tại sao phải có *broker* ở giữa? Và tại sao lại là **Kafka** chứ không phải RabbitMQ, ActiveMQ? Hiểu được chỗ này là hiểu được **bản chất khác biệt của Kafka** — thứ chi phối mọi quyết định thiết kế về sau.

## 1. Nếu KHÔNG có broker thì sao?

Giả sử các service tự gọi thẳng nhau (point-to-point). Khi Account có sự kiện "tiền vừa chuyển", nó phải tự gọi Ledger, Fraud, Notification:

```
            ┌──▶ Ledger
Account ────┼──▶ Fraud
            └──▶ Notification
```

Vấn đề nảy sinh:
- Account phải **biết địa chỉ** của cả 3 → coupling chặt trở lại.
- Thêm service thứ 4 → **phải sửa code Account**.
- Nếu Fraud đang chết → Account phải tự lo retry, buffer, lưu lại để gửi sau → mỗi service tự code lại logic này.
- Account phải chờ cả 3 → chậm, dễ sập dây chuyền (đúng vấn đề 0.2).

**Broker sinh ra để gánh hết những việc này.** Nó là một *trung gian* chuyên trách nhận – lưu – chuyển phát message, để các service không phải biết nhau và không phải tự lo độ tin cậy.

## 2. Message Broker giải quyết 4 việc cốt lõi

| Việc | Ý nghĩa |
|---|---|
| **Decoupling** | Người gửi và người nhận không biết nhau, chỉ biết broker. |
| **Buffering** | Message được giữ lại khi người nhận bận/chết → chịu tải đột biến (load leveling). |
| **Reliability** | Broker lưu bền message, đảm bảo không mất, chuyển phát lại khi cần. |
| **Routing / fan-out** | Một message tới đúng (các) người cần nhận. |

## 3. Hai "trường phái" broker — đây là chỗ then chốt

Đây là kiến thức phân biệt người hiểu Kafka sâu với người chỉ "dùng được".

### 3a. Broker truyền thống — *Smart broker, dumb consumer* (RabbitMQ, ActiveMQ)

Mô hình **message queue**: broker đẩy (*push*) message tới consumer, và **message bị xóa sau khi được xử lý xong** (ack).

```
Producer ─▶ [ msg3 | msg2 | msg1 ] ─▶ Consumer
              (hàng đợi)              (nhận xong → XÓA msg)
```

- Broker "thông minh": lo routing phức tạp, theo dõi ai đã nhận, ai chưa.
- Message **dùng một lần rồi mất**. Đã xử lý xong là biến mất khỏi queue.
- Khó có chuyện "đọc lại lịch sử" hay "service mới muốn đọc từ đầu".

### 3b. Kafka — *Dumb broker, smart consumer* — kiến trúc **Log**

Đây là điểm cách mạng. Kafka **không phải là một hàng đợi (queue)** theo nghĩa truyền thống. Kafka là một **distributed, append-only commit log** (sổ ghi chỉ-thêm-vào, phân tán, bền vững).

```
        Partition (một file log, chỉ ghi nối đuôi):
offset:  0    1    2    3    4    5    6
        [e0]─[e1]─[e2]─[e3]─[e4]─[e5]─[e6] ◀── producer ghi vào CUỐI
         ▲              ▲
         │              │
   Consumer-A      Consumer-B          ← mỗi consumer tự giữ vị trí đọc (offset)
   (đọc offset 1)  (đọc offset 4)         ĐỘC LẬP nhau
```

Khác biệt cốt lõi so với queue truyền thống:

| | Queue truyền thống (RabbitMQ) | **Log (Kafka)** |
|---|---|---|
| Đọc xong message | **Bị xóa** | **Vẫn còn**, lưu theo thời gian (retention) |
| Ai theo dõi tiến độ đọc | **Broker** | **Consumer** (tự giữ *offset*) |
| Nhiều người đọc cùng data | Khó (queue chia message ra) | Dễ — ai cũng đọc được toàn bộ log, độc lập |
| Đọc lại (replay) lịch sử | Không | **Có** — chỉ cần tua offset về |
| Model push/pull | Broker **push** | Consumer **pull** |
| Thế mạnh | Routing linh hoạt, task queue | **Throughput cực cao, replay, fan-out, lưu trữ** |

**Điểm "aha" cần thấm:** Trong Kafka, *đọc một message không tiêu hủy nó*. Message giống một **dòng trong sổ cái**, nằm đó cho tới khi hết hạn retention. 10 service khác nhau đọc cùng một event, mỗi service một offset riêng, không ai ảnh hưởng ai. Đây chính là thứ làm cho **event-driven fan-out** (mục 0.2) trở nên tự nhiên và rẻ.

## 4. Vì sao đặc tính "Log" này lại quan trọng đến vậy với chúng ta

Domain Banking của ta hưởng lợi trực tiếp từ 4 tính chất của log:

1. **Replay** — Service Fraud mới ra đời, muốn phân tích lại 30 ngày giao dịch quá khứ? Chỉ cần tua offset về đầu và đọc lại. Queue truyền thống không làm được vì data đã bị xóa.
2. **Multiple consumers độc lập** — Cùng một event `MoneyTransferred`, Ledger + Fraud + Notification đọc song song, mỗi bên một nhịp.
3. **Audit / source of truth** — Log bất biến, chỉ-thêm chính là một **sổ cái tự nhiên** — cực hợp với ngân hàng (nền tảng cho *Event Sourcing* ở Module 6).
4. **Throughput khổng lồ** — Kafka ghi tuần tự xuống đĩa (sequential I/O) nên nhanh kinh khủng, chịu được hàng triệu message/giây.

## 5. Kafka đứng ở đâu trong bức tranh tổng thể?

Kafka đóng vai **"hệ thần kinh trung ương" (central nervous system)** của kiến trúc event-driven: mọi thay đổi trạng thái quan trọng trong hệ chảy qua Kafka dưới dạng event, và bất kỳ service nào quan tâm đều có thể lắng nghe.

```
   ┌─────────┐   events   ┌──────────────────────┐   events   ┌──────────┐
   │ Account │ ─────────▶ │        KAFKA          │ ─────────▶ │  Ledger  │
   │ Service │            │  (central event log)  │            │ Service  │
   └─────────┘            │                       │ ─────────▶ ┌──────────┐
   ┌─────────┐ ─────────▶ │                       │            │  Fraud   │
   │Transaction          │                       │ ─────────▶ ┌──────────┐
   └─────────┘            └──────────────────────┘            │Notify    │
```

> ⚖️ **Cân bằng góc nhìn senior:** Kafka *không phải* luôn là lựa chọn đúng. Nếu bạn chỉ cần một *task queue* đơn giản (gửi email, xử lý job nền) với routing phức tạp và không cần replay/throughput cao, thì **RabbitMQ thường đơn giản và phù hợp hơn**. Kafka tỏa sáng khi bạn cần: **throughput cao, replay, event sourcing, nhiều consumer độc lập, lưu giữ event như nguồn sự thật**. Domain Banking của ta cần đúng những thứ đó — nên Kafka là lựa chọn xác đáng.

---

📌 **Tóm tắt Bài 0.3:** Broker gánh decoupling + buffering + reliability + routing để service không phải tự lo. Broker truyền thống (RabbitMQ) là *queue*: đọc xong xóa, broker giữ tiến độ. Kafka là *distributed append-only log*: đọc không xóa, **consumer tự giữ offset**, cho phép **replay, fan-out nhiều consumer độc lập, throughput cực cao, và log như nguồn sự thật** — đúng những gì hệ Banking cần.

**Hỏi đáp 0.3:** _(Học viên không có câu hỏi.)_

---

# Bài 0.4 — Bài toán nhất quán dữ liệu phân tán

## 1. Đặt lại bài toán bằng domain của chúng ta

Nhắc lại tình huống ở 0.1, giờ mổ xẻ kỹ:

> Chuyển **1.000.000đ** từ ví A sang ví B. Ví A do **Account Service** quản lý (DB_A). Ví B cũng vậy nhưng — trong nhiều thiết kế — việc *ghi sổ cái* lại do **Ledger Service** (DB_Ledger) lo, *kiểm tra gian lận* do **Fraud Service** lo. Một giao dịch "chuyển tiền" thực ra phải **thay đổi trạng thái ở nhiều service / nhiều DB**.

Trong **monolith**, việc này tầm thường:

```java
@Transactional                 // MỘT transaction ôm trọn
public void transfer(...) {
    accountRepo.debit(A, amount);   // trừ A
    accountRepo.credit(B, amount);  // cộng B
    ledgerRepo.record(...);         // ghi sổ
}                                   // commit → cả 3 cùng thành công, hoặc cùng rollback
```

Database lo cho ta tính chất **ACID**: hoặc *tất cả* thành công, hoặc *tất cả* bị hủy. Không bao giờ có chuyện "trừ A xong nhưng chưa cộng B".

**Khi tách ra microservices, điều kỳ diệu đó BIẾN MẤT.** A nằm DB_A, sổ nằm DB_Ledger — hai database khác nhau, hai process khác nhau, nối với nhau qua mạng. Không có một `@Transactional` nào ôm được cả hai.

## 2. Tại sao không dùng "distributed transaction" (2PC)?

Có một kỹ thuật cổ điển để làm transaction xuyên nhiều DB: **Two-Phase Commit (2PC / XA)**. Ý tưởng: có một *coordinator* hỏi tất cả "mọi người sẵn sàng commit chưa?" (phase 1 — prepare), nếu tất cả đồng ý thì ra lệnh "commit!" (phase 2).

```
            Phase 1: PREPARE          Phase 2: COMMIT
Coordinator ──"sẵn sàng?"──▶ A        ──"commit!"──▶ A
            ──"sẵn sàng?"──▶ Ledger   ──"commit!"──▶ Ledger
            ◀──"OK"────────           
```

Nghe hay, nhưng trong microservices người ta **gần như luôn tránh 2PC**. Lý do senior cần biết:

| Vấn đề của 2PC | Hệ quả |
|---|---|
| **Synchronous blocking** | Trong lúc "prepare", các resource bị **khóa** (lock). Mọi service phải chờ nhau → chậm, giảm throughput nghiêm trọng. |
| **Coordinator là single point of failure** | Coordinator chết giữa chừng → các participant kẹt ở trạng thái "đã prepare, chưa biết commit hay không" → khóa treo. |
| **Coupling thời gian chặt** | Tất cả service phải **sống và sẵn sàng cùng lúc** — phá vỡ chính lợi ích decoupling ta vừa giành được ở 0.2. |
| **Không scale & nhiều DB/broker không hỗ trợ** | Kafka **không** hỗ trợ XA theo kiểu này. |

> 💡 Kết luận của giới senior: *2PC đánh đổi tính sẵn sàng (availability) để lấy tính nhất quán mạnh (strong consistency), và trong hệ phân tán quy mô lớn, cái giá đó quá đắt.* → Ta cần một cách khác.

## 3. Định lý CAP — vì sao "không khác được"

Đây là định lý nền tảng giải thích **vì sao** ta buộc phải đánh đổi. **CAP theorem** (Eric Brewer) nói: một hệ dữ liệu phân tán **không thể đồng thời đảm bảo cả ba**:

- **C — Consistency** (nhất quán): mọi node đọc ra **cùng một giá trị mới nhất** tại mọi thời điểm.
- **A — Availability** (sẵn sàng): mọi request đều nhận được phản hồi (không lỗi, không treo).
- **P — Partition tolerance** (chịu chia cắt mạng): hệ vẫn hoạt động khi **mạng giữa các node bị đứt/chậm**.

```
        C
       / \
      /   \        Khi network partition (P) xảy ra — mà trong
     /     \       hệ phân tán nó CHẮC CHẮN sẽ xảy ra — bạn
    A───────P      buộc phải CHỌN giữ C hay giữ A.
```

**Mấu chốt mà nhiều người hiểu sai:** Trong hệ phân tán thật, **P là bắt buộc** — mạng *sẽ* có lúc đứt, gói tin *sẽ* có lúc trễ. Bạn không được phép "chọn bỏ P". Vậy khi partition xảy ra, bạn chỉ thực sự chọn giữa:

- **CP** (chọn Consistency, hy sinh Availability): khi mạng đứt, **từ chối phục vụ** để không trả ra dữ liệu sai. Ví dụ: nhiều DB quan hệ truyền thống.
- **AP** (chọn Availability, hy sinh Consistency tức thời): khi mạng đứt, **vẫn phục vụ**, chấp nhận các node tạm thời lệch nhau, rồi *hội tụ về đúng sau* — đây chính là **eventual consistency**.

Kiến trúc event-driven với Kafka nghiêng về phía **AP + eventual consistency**: hệ luôn nhận lệnh, luôn phản hồi nhanh, và các service *dần dần* đồng bộ trạng thái qua event.

## 4. Eventual Consistency — chấp nhận "đúng sau một lúc"

**Eventual consistency** (nhất quán *cuối cùng*): hệ thống đảm bảo rằng *nếu không có thay đổi mới, thì sau một khoảng thời gian, tất cả các bản sao/trạng thái sẽ hội tụ về cùng một giá trị đúng.* Không phải "đúng ngay tức khắc", mà là **"đúng cuối cùng"**.

Quay lại chuyển tiền A→B theo kiểu event-driven:

```
t0: Account trừ A (-1tr) trong DB_A  ──┐
                                       ├─ ghi cả event "MoneyDebited" trong CÙNG transaction (Outbox - M6)
t0: lưu event vào DB_A                ──┘
t1: event "MoneyDebited" lên Kafka
t2: Ledger đọc event → ghi sổ cái
t3: Notification đọc event → gửi SMS
```

Giữa **t0 và t2**, hệ thống **không nhất quán**: A đã bị trừ nhưng sổ cái chưa ghi. Đây là **"inconsistency window"** (cửa sổ không nhất quán). Eventual consistency chấp nhận cửa sổ này tồn tại (thường vài mili-giây tới vài giây), miễn là cuối cùng mọi thứ hội tụ đúng.

> ⚠️ **Đây là thay đổi tư duy lớn nhất khi rời monolith.** Bạn phải tập chấp nhận: "trong một khoảnh khắc ngắn, các phần của hệ thống nhìn thấy trạng thái khác nhau — và điều đó *là bình thường*, miễn là ta thiết kế để chúng hội tụ đúng và xử lý được cửa sổ đó."

## 5. Hệ quả thiết kế: 3 thứ trở thành BẮT BUỘC

Vì đã chấp nhận eventual consistency và giao tiếp qua mạng (có thể mất gói, gửi lặp), ba kỹ thuật sau **không còn là tùy chọn** — chúng là điều kiện sống còn. Đây chính là *lý do tồn tại* của các module sau:

| Kỹ thuật | Giải quyết điều gì | Học ở đâu |
|---|---|---|
| **Atomicity giữa DB và event** (Transactional Outbox) | Làm sao "trừ A" và "phát event MoneyDebited" cùng thành công hoặc cùng thất bại, khi DB và Kafka là hai hệ khác nhau? | Module 6.1 |
| **Idempotency** | Kafka đảm bảo *at-least-once* → một event có thể tới **2 lần**. Phải đảm bảo "cộng tiền B" chạy 2 lần vẫn ra kết quả đúng (không cộng đôi). | Module 5.4 |
| **Saga** (đền bù) | Không có rollback xuyên service. Nếu bước 3 thất bại, phải chạy **bước đền bù** (compensating action) để hoàn tác bước 1, 2. | Module 6.2–6.3 |

Toàn bộ "phần khó" của Kafka mà bạn chọn học (exactly-once, idempotency, consistency) **bắt nguồn từ chính bài 0.4 này**. Mọi thứ về sau là *công cụ để sống chung với eventual consistency một cách an toàn*.

## 6. Một nguyên tắc senior để gối đầu giường

> **"Embrace eventual consistency, but design for correctness."**
> Chấp nhận rằng hệ sẽ không nhất quán tức thời — nhưng thiết kế sao cho nó **luôn hội tụ về trạng thái đúng**, và **không bao giờ** mất tiền hay tạo tiền (trong ngân hàng, đây là ranh giới sống còn). Eventual consistency *không* có nghĩa là "chấp nhận sai số"; nó có nghĩa là "đúng, nhưng cho phép trễ".

---

📌 **Tóm tắt Bài 0.4:** Tách microservices làm mất ACID transaction xuyên service. 2PC giải được nhưng đánh đổi availability + coupling quá đắt nên bị tránh. CAP theorem chỉ ra: khi mạng có thể đứt (P bắt buộc), phải chọn giữa C và A — kiến trúc Kafka chọn **AP + eventual consistency**: luôn phản hồi, chấp nhận "cửa sổ không nhất quán", miễn là **hội tụ đúng cuối cùng**. Hệ quả: **Outbox, Idempotency, Saga** trở thành bắt buộc — đó là lý do tồn tại của các module sau.

**Hỏi đáp 0.4:** _(Học viên không có câu hỏi.)_

---

# Bài 0.5 — Thiết kế domain Banking/Wallet

Đây là lúc lý thuyết 0.1–0.4 biến thành **bản thiết kế cụ thể** mà ta sẽ code. Tôi sẽ dùng một chút tư duy **Domain-Driven Design (DDD)** — không hàn lâm, chỉ đủ để chia service cho đúng.

## 1. Nguyên tắc chia service: Bounded Context

Sai lầm kinh điển của người mới: chia service theo **danh từ kỹ thuật** ("User service", "Database service") hoặc theo **tầng** ("Controller service", "DAO service"). Đó là chia sai.

Cách đúng: chia theo **Bounded Context** — mỗi service bao trọn **một năng lực nghiệp vụ (business capability)** có ranh giới rõ ràng, sở hữu dữ liệu của riêng nó, và nói "ngôn ngữ" nghiệp vụ riêng.

> 💡 Quy tắc kiểm tra của senior: *"Nếu hai khối logic thay đổi vì hai lý do nghiệp vụ khác nhau, và do hai team khác nhau sở hữu, thì chúng nên là hai service."* (giống Single Responsibility nhưng ở cấp service).

## 2. Các Bounded Context của hệ Banking/Wallet

Tôi đề xuất 4 service nghiệp vụ chính + một số thành phần hỗ trợ:

| Service | Sở hữu (nguồn sự thật) | Trách nhiệm | KHÔNG làm |
|---|---|---|---|
| **Account Service** | Tài khoản ví, **số dư** (balance) | Mở ví, giữ số dư, thực hiện debit/credit lên ví | Không quyết định một "giao dịch chuyển tiền" hoàn tất hay chưa |
| **Transaction Service** | **Vòng đời giao dịch** (transfer) | Điều phối luồng chuyển tiền (orchestrator của Saga), giữ trạng thái transaction | Không giữ số dư |
| **Ledger Service** | **Sổ cái kế toán** (double-entry) | Ghi nhận bất biến mọi bút toán nợ/có, phục vụ audit & đối soát | Không sửa số dư, không điều phối |
| **Fraud Service** | Quy tắc & lịch sử rủi ro | Phân tích giao dịch, chấp thuận/từ chối theo luật chống gian lận | Không giữ tiền |

Thành phần hỗ trợ (không phải bounded context nghiệp vụ lõi):
- **Notification Service** — nghe event, gửi SMS/email (consumer thuần, dễ minh hoạ fan-out).
- **API Gateway** — điểm vào REST cho client (sẽ bàn ở module sau, có thể giản lược).

> ⚖️ **Quyết định thiết kế quan trọng — và tôi muốn hỏi ý bạn ở mục 6 bên dưới:** "số dư" (balance) nên nằm ở Account Service hay được suy ra từ Ledger? Đây là một quyết định kiến trúc thật sự, không có đáp án "đúng tuyệt đối".

## 3. Phân biệt 3 khái niệm rất hay bị gộp nhầm

Trong domain tiền tệ, ba thứ này **khác nhau** và phải tách bạch:

- **Balance (số dư)** — trạng thái *hiện tại* "ví A còn bao nhiêu". Đây là *state*, do **Account Service** giữ.
- **Transaction (giao dịch)** — một *ý định nghiệp vụ* "chuyển 1tr từ A sang B", có vòng đời (PENDING → COMPLETED/FAILED). Do **Transaction Service** giữ.
- **Ledger entry (bút toán)** — bản ghi *kế toán bất biến* "ghi nợ A 1tr / ghi có B 1tr", không bao giờ sửa/xóa. Do **Ledger Service** giữ.

Một lần chuyển tiền thành công sẽ tạo ra: **1 transaction** (vòng đời) → tác động lên **2 balance** (A giảm, B tăng) → ghi **2 bút toán** đối ứng trong ledger (double-entry).

## 4. Luồng nghiệp vụ trung tâm: Transfer Money (chuyển tiền)

Đây là luồng "xương sống" ta sẽ code dần qua các module. Nó chạm vào **mọi** khái niệm khó của khóa. Phác thảo ở mức ý niệm (chi tiết Saga để Module 6):

```
                                  ┌──────────────────┐
   (1) POST /transfers           │   Transaction    │
 Client ───────────────────────▶ │     Service      │  tạo Transaction = PENDING
                                  └────────┬─────────┘
                                           │ (2) event: TransferRequested
                                           ▼
                                       [ KAFKA ]
                          ┌────────────────┼─────────────────┐
                          ▼                ▼                 ▼
                  ┌──────────────┐  ┌─────────────┐   ┌──────────────┐
                  │    Fraud     │  │   Account   │   │    Ledger    │
                  │  (3) check   │  │ (4) debit A │   │ (6) ghi bút  │
                  │  approve/deny│  │   credit B  │   │    toán      │
                  └──────┬───────┘  └──────┬──────┘   └──────────────┘
                         │ event           │ event
                         │ FraudChecked    │ MoneyDebited / MoneyCredited
                         ▼                 ▼
                      [ KAFKA ] ────▶ Transaction Service
                                       (5) cập nhật trạng thái: COMPLETED / FAILED
                                       → nếu FAILED sau khi đã debit: phát lệnh hoàn tiền (compensation)
```

Những điểm "khó" lộ ra ngay ở luồng này — và đó là chủ đích:
- **(2)+(4): Outbox** — Account vừa đổi số dư trong DB *vừa* phát event, phải nguyên tử (Module 6.1).
- **(4): Idempotency** — event `TransferRequested` có thể tới 2 lần, không được trừ tiền 2 lần (Module 5.4).
- **(5): Saga** — Transaction Service điều phối; nếu Fraud từ chối *sau khi* đã trừ A, phải **compensate** (hoàn tiền A) (Module 6.2–6.3).
- **Ledger**: nguồn audit bất biến, hợp với bản chất log của Kafka.

## 5. Phác thảo Topic & Event (xem trước, sẽ chốt ở Module 2–3)

Mỗi loại event nghiệp vụ → một **topic** Kafka. Quy ước đặt tên kiểu `<domain>.<aggregate>.<event>` (sẽ giải thích kỹ ở Module 2):

| Topic (dự kiến) | Producer | Consumer chính | Event |
|---|---|---|---|
| `banking.transfer.requested` | Transaction | Fraud, Account | `TransferRequested` |
| `banking.fraud.checked` | Fraud | Transaction | `FraudChecked` (APPROVED/DENIED) |
| `banking.account.debited` | Account | Transaction, Ledger, Notification | `MoneyDebited` |
| `banking.account.credited` | Account | Transaction, Ledger, Notification | `MoneyCredited` |
| `banking.transfer.completed` | Transaction | Notification | `TransferCompleted` |

> Đây mới là *phác thảo*. Số partition, key, retention, compaction sẽ được quyết định có chủ đích ở Module 1–2 (vì chúng gắn với ordering & throughput).

## 6. Cấu trúc source code dự kiến (multi-module Maven)

```
source/
├── pom.xml                      (parent POM, quản lý version chung)
├── common-events/               (Avro schema + class sinh ra, dùng chung)
├── account-service/
├── transaction-service/
├── ledger-service/
├── fraud-service/
└── notification-service/
```

Mỗi service là một Spring Boot app độc lập, DB riêng. `common-events` chứa **contract** (Avro schema) — chính là "hợp đồng" giữa các service mà ta đã nói ở 0.2.

---

📌 **Tóm tắt Bài 0.5:** Chia service theo **bounded context** (năng lực nghiệp vụ), không theo tầng kỹ thuật. Hệ Banking gồm 4 context lõi: **Account** (số dư), **Transaction** (vòng đời giao dịch / orchestrator), **Ledger** (sổ cái bất biến), **Fraud** (rủi ro), cộng Notification. Phân biệt rõ **balance / transaction / ledger entry**. Luồng trung tâm **Transfer Money** chạm vào mọi khái niệm khó (Outbox, Idempotency, Saga) — sẽ được code dần. Source tổ chức multi-module Maven, có `common-events` giữ Avro contract.

**Hỏi đáp 0.5:**

> **Câu hỏi của mentor cho học viên:** Số dư (balance) nên giữ trực tiếp ở Account Service, hay suy ra từ Ledger (event sourcing)?
>
> **Học viên trả lời:** "Bạn quyết định giúp tôi."
>
> **Quyết định & lý do (mentor):** Chọn **Account giữ balance trực tiếp + Ledger ghi bút toán song song để audit**, rồi tiến hóa sang materialized view suy-ra-từ-ledger + reconciliation ở Module 6/7. Lý do:
> 1. **Đi từ đơn giản đến phức tạp là kỷ luật kỹ thuật, không phải lười** — nắm chắc producer/consumer/Outbox/idempotency trước, không bị event sourcing làm nhiễu; hiểu "vì sao cần CQRS" sau khi đã cảm nhận nỗi đau của cách làm đơn giản.
> 2. **Sát cách hệ thật vận hành** — nhiều ngân hàng/ví giữ bảng balance "nóng" (đọc nhanh, có khóa) *cộng với* ledger bất biến để đối soát, thay vì tính lại số dư từ hàng triệu bút toán mỗi query.
> 3. **Dựng sẵn sân khấu cho Module 6/7** — có cả balance lẫn ledger thì minh hoạ được CQRS (materialized view số dư từ ledger) và bài học reconciliation (đối chiếu hai con số).
>
> Nguyên tắc nhất quán: Account đổi balance *và* phát event trong cùng transaction (**Outbox** — M6.1); Ledger là consumer ghi bút toán; balance & ledger hội tụ theo **eventual consistency**, có job đối soát phát hiện lệch.

---

✅ **Kết thúc MODULE 0.** Tiếp theo: Module 1 — Kafka Core Concepts (lý thuyết sâu).

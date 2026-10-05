# Bonus B2 — Flywheel dữ liệu cho chatbot CSKH SaaS tiếng Việt

## Bài toán và ràng buộc

Thiết kế này dành cho một chatbot hỗ trợ khách hàng của sản phẩm SaaS tại Việt Nam.
Người dùng hỏi về đăng nhập, thanh toán, cấu hình và lỗi sản phẩm; agent có thể gọi
công cụ để tra cứu tài khoản hoặc tạo ticket. Mục tiêu của pipeline là biến trace sản
phẩm thành ba đầu ra: dashboard vận hành hằng ngày, eval set đáng tin cậy và dữ liệu
huấn luyện theo cặp ưu tiên. Dữ liệu đến từ OpenTelemetry trace, ticket CRM, phản hồi
thumbs-up/down và transcript bàn giao cho nhân viên. Khó khăn chính là trace lồng nhau,
schema thay đổi khi agent có tool mới, phản hồi người dùng thưa và thiên lệch, văn bản có
PII, đồng thời một lỗi lọt vào tập train có thể được khuếch đại ở model tiếp theo.

Giả định quy mô ban đầu là 50.000 hội thoại/ngày, giữ raw 90 ngày, dashboard trễ tối đa
15 phút nhưng tập eval/train chỉ cần cập nhật hằng đêm. Nhóm gồm hai data engineer và
một ML engineer, nên thiết kế ưu tiên khả năng kiểm chứng và vận hành đơn giản hơn độ
trễ vài giây.

```text
Agent/CRM/feedback
        │
        ▼
Kafka + object storage (Bronze bất biến, partition ingest_date)
        │
        ├──► stream validation ─► quarantine + cảnh báo
        │
        ▼
Silver: conversation/span/tool_call/feedback đã chuẩn hoá, PII tokenized
        │
        ├──► near-real-time metrics (15 phút)
        │
        └──► nightly point-in-time join
                    │
                    ├──► versioned eval set + holdout registry
                    └──► candidate DPO/SFT ─► dedup/decontam ─► human review
                                                         │
                                                         ▼
                                                  dataset registry
```

## 1. Batch hay streaming?

Quyết định là mô hình lai có một đường ingest streaming nhưng chỉ một nguồn sự thật
Bronze. Streaming phục vụ cảnh báo tỷ lệ lỗi tool, latency và quarantine trong vòng 15
phút; việc dựng eval/DPO chạy batch ban đêm. So với streaming toàn bộ, cách này chấp
nhận dataset mới chậm tối đa một ngày để đổi lấy replay dễ, join point-in-time rõ và ít
trạng thái phân tán. So với batch thuần, nó vẫn phát hiện sớm sự cố production có thể
làm hỏng hàng chục nghìn phiên. Không xây hai logic transform độc lập: batch đọc lại
cùng event contract và cùng mã chuẩn hoá mà consumer streaming sử dụng.

## 2. Hợp đồng dữ liệu và dòng xấu đi đâu?

Bronze giữ payload nguyên bản cùng `trace_id`, `span_id`, schema version, thời điểm sự
kiện và thời điểm ingest. Silver yêu cầu khóa không rỗng, quan hệ parent-child hợp lệ,
tool name thuộc registry, timestamps có thứ tự hợp lý và output tool khớp JSON Schema
theo version. Record sai không bị bỏ âm thầm và cũng không làm dừng toàn bộ batch: nó
vào quarantine có reason code, payload hash và lineage. Cảnh báo mở khi tỷ lệ quarantine
vượt 0,5% trong 15 phút hoặc một reason mới xuất hiện. Data engineer chịu trách nhiệm
schema/transport; owner của tool chịu trách nhiệm output sai. Đánh đổi là phải duy trì
registry và quy trình tương thích ngược, nhưng đổi lại schema drift trở thành tín hiệu
quan sát được thay vì lỗi chất lượng model vài tuần sau.

## 3. Train/serve parity và chống rò rỉ tương lai

Mỗi ví dụ được dựng bằng ASOF join tại `conversation_ended_at`: chỉ ticket state,
knowledge version và feature đã tồn tại lúc agent trả lời mới được dùng. Feedback đến
sau được phép làm nhãn nhưng không được đi vào feature đầu vào. Dataset lưu `as_of`,
query version, source hashes và checksum; bản đã publish là bất biến. Đây phức tạp hơn
join trạng thái mới nhất, nhưng ngăn việc model “biết trước” ticket sẽ escalated hoặc
khách sẽ hoàn tiền. Cùng hàm feature được đóng gói và dùng cho offline build lẫn service
online; parity test chạy trên một tập conversation cố định ở mỗi release.

## 4. Flywheel mà không tự đầu độc

Không coi thumbs-up là chân lý duy nhất. Candidate tốt cần phối hợp outcome thực tế,
rubric tự động và sampling: giải quyết không cần reopen trong bảy ngày, không vi phạm
policy, citation hợp lệ và latency trong SLO. Candidate xấu gồm tool failure, human
correction hoặc câu trả lời bị downvote. Trước khi tạo DPO pair, pipeline deduplicate
theo normalized prompt hash và dùng n-gram/embedding để decontaminate với eval holdout.
Một mẫu từ eval hoặc gần trùng eval tuyệt đối không vào train. Mẫu nhạy cảm, bất đồng
giữa các tín hiệu hoặc rubric confidence thấp đi qua human review. Đánh đổi là thu được
ít dữ liệu hơn và tốn công duyệt, nhưng precision quan trọng hơn volume vì một nhãn sai
có thể được lặp lại qua nhiều vòng flywheel.

## 5. Idempotency, backfill và quyền riêng tư Việt Nam

Mọi bảng thực thể dùng khóa ổn định; event bất biến dedup bằng `(source, event_id)`;
partition tổng hợp được overwrite trong cửa sổ lookback đo từ lateness. Side effect
không thể đảo ngược là publish dataset cho training, nên bước đó cần checksum, approval
và dataset version mới thay vì ghi đè. Backfill chạy cùng code path, từ ngày cũ đến mới,
và phải chứng minh checksum trước/sau khi rerun không đổi.

PII được tách khỏi nội dung càng sớm càng tốt: email, điện thoại, mã khách hàng được
tokenize bằng khóa nằm ngoài lake; tên người và địa chỉ dùng Vietnamese NER cộng rule,
sau đó sample thủ công. Bronze raw mã hóa, quyền đọc tối thiểu và TTL 90 ngày; dataset
train không chứa mapping phục hồi token. Chất lượng chốt PII được đo bằng recall trên
golden set tiếng Việt có dấu/không dấu và leak rate trên sample production, thay vì chỉ
đếm regex match. Chi phí thêm là false positive làm mất ngữ cảnh, nên policy cho phép
giữ các thực thể sản phẩm công khai qua allowlist có version.

## Phương án bị loại

Tôi loại kiến trúc Lambda với một pipeline streaming và một pipeline batch viết riêng.
Nó cho latency thấp nhưng tạo hai định nghĩa khác nhau cho cùng một feature, tăng gấp
đôi chi phí kiểm thử và dễ làm dashboard khác dataset training. Tôi cũng không dùng LLM
judge làm nhãn duy nhất: judge có thể thiên lệch giống model đang huấn luyện và thay đổi
theo version. LLM judge vẫn hữu ích như một tín hiệu có cache theo input hash, model và
prompt version, nhưng quyết định publish phải dựa trên nhiều tín hiệu cùng audit sample.

## Khi tăng quy mô

Ở 10×, bottleneck đầu tiên dự kiến là small files và bước human review, không phải SQL.
Pipeline sẽ compact Bronze theo giờ, cluster Silver theo ngày và `trace_id`, đồng thời
ưu tiên review bằng risk score. Ở 100× mới cân nhắc chuyển transform nặng sang Spark và
object-table format; không chọn Spark ngay từ đầu vì chi phí vận hành không tương xứng
với quy mô và năng lực nhóm hiện tại.

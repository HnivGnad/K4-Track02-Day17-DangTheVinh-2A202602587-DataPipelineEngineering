# K4-Track02-Day17 — Report cá nhân

**Họ tên / MSSV:** Đặng Thế Vinh / 2A202602587

**Repo:** https://github.com/HnivGnad/K4-Track02-Day17-Data-Pipeline-Engineering

**Commit bài nộp:** `<bổ sung sau commit cuối>`

**AI đã dùng và phạm vi hỗ trợ:** OpenAI Codex hỗ trợ đọc đề/code, sửa ba lỗi,
review diff, chạy kiểm thử, triển khai bonus và soạn báo cáo; tôi review kết quả và
cần giải thích được các thay đổi trước khi nộp.

**Nguồn tham khảo khác:** Tài liệu và tests có sẵn trong repository; không dùng dữ liệu
hay lời giải bên ngoài.

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `silver_tickets` có nhiều hàng cho cùng ticket; T-91 không còn duy nhất; checksum đổi khi rerun. | Feature của u05 ngày 12/08 thiếu event đến ngày 15/08; checksum không khớp full recompute. | T-97 vẫn tồn tại trong Silver, latest training snapshot và RAG chunks. |
| **Nguyên nhân gốc** | Batch chỉ `INSERT`; không có entity key và không chặn batch cũ ghi đè trạng thái mới. | `LOOKBACK_DAYS = 0`, nên run ngày 15/08 không dựng lại partition event-time 12/08. | Debezium delete có `after=null`, nhưng staging chỉ lấy `ticket_id` từ `after`, nên record delete bị lọc mất. |
| **Cách sửa** | `pipeline/silver.py`: `MERGE` theo `ticket_id`; update chỉ khi `s._lsn > t._lsn`. | `pipeline/config.py`: đặt lookback 3 ngày theo `ceil(P99)` đo từ Bronze; Gold overwrite cửa sổ event-time. | `pipeline/staging.py`: `coalesce(after.ticket_id, before.ticket_id)`; Silver lưu tombstone với PII null để delete lan xuống Gold. |
| **Khái niệm trên slide** | Silver có khoá; keyed upsert; LSN guard; idempotent replay. | Event time khác ingest time; đo lateness; recompute partition bằng lookback. | Đọc đúng CDC envelope; Kafka tombstone khác CDC delete; “xoá phải lan”. |

## 2. Các con số

- P99 lateness đo từ 43 record Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`.
- `submission/checksums.txt`: **PASS** — Gold checksum `39e115c510ecdf526800eac227158a4f`.
- `make parity`: **PARITY**.

## 3. Lựa chọn công cụ / kỹ thuật

- Dùng `MERGE` theo khoá cho entity `silver_tickets` để update có LSN guard; dùng
  overwrite-partition cho feature aggregate vì late event có thể đổi toàn bộ user-day.
- Giữ tombstone Silver thay vì hard delete để lưu LSN, chống replay batch cũ làm ticket
  “sống lại”, nhưng xóa các cột PII và loại ticket khỏi các Gold serving table.
- Dựng training snapshot từ Bronze “as of” ngày đó để tái lập point-in-time và không
  sửa dataset đã dùng huấn luyện; một thay đổi hợp lệ tạo version mới.
- Chọn DuckDB cho pipeline nhỏ, local, zero-key và dễ replay; dùng dbt để kiểm tra cùng
  logic bằng contracts/merge/microbatch. Spark chỉ đáng chi phí vận hành khi scale lớn.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot bất biến phục vụ reproducibility không nên là lý do giữ PII trái yêu cầu
   xóa. Tôi tách nội dung nhạy cảm khỏi metadata/version: crypto-shred khóa mã hóa hoặc
   duy trì deletion manifest bắt buộc áp dụng khi đọc; sau đó publish snapshot đã redacted
   thành version mới và lưu audit lineage. Model đã train từ dữ liệu đó cần được đánh giá
   mức ảnh hưởng, retrain/unlearn theo policy và ghi nhận ngoại lệ pháp lý nếu có.
2. Tôi đặt chốt PII ngay khi Bronze đi sang Silver: regex cho email/điện thoại kết hợp
   Vietnamese NER cho tên/địa chỉ và allowlist thực thể sản phẩm. Record rủi ro cao vào
   quarantine/human review trước khi tới Gold. Đo precision/recall trên golden set có
   tiếng Việt có dấu/không dấu, leak rate trên sample production và cảnh báo khi drift.

## 5. Output thực tế

### Verify

```text
> .\.venv\Scripts\python.exe -m scripts.verify
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build
RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt
```

### Pytest

```text
> .\.venv\Scripts\python.exe -m pytest -o addopts=''
collected 34 items
tests\test_contracts.py .............                                    [ 38%]
tests\test_extensions.py ...........                                     [ 70%]
tests\test_rerun.py .                                                    [ 73%]
tests\test_units.py .........                                            [100%]
============================= 34 passed in 2.23s ==============================
```

### Rerun checksum

```text
> .\.venv\Scripts\python.exe -m scripts.rerun_check
run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
RESULT: PASS — 3 re-runs, identical checksums
```

### Lateness

```text
> .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3
```

### dbt build và parity

```text
> dbt build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models.
Completed successfully
Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

> .\.venv\Scripts\python.exe -m scripts.parity
[OK] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
[OK] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

### Bonus B1 — LLM transform

```text
> .\.venv\Scripts\python.exe -m scripts.bonus_llm
=== bonus: LLM labelling of 11 live tickets ===
cost estimate before running: ~484 tokens = $0.0010 per full run
[OK] first run labels every live ticket
[OK] re-run with same model + prompt makes 0 LLM calls
[OK] every Gold label is bug / billing / other
[OK] off-schema answers go to llm_label_quarantine
[OK] new prompt version re-labels on purpose
[OK] labels carry their prompt version
BONUS PASS
```

### Bonus B2 — Thiết kế

Xem [`bonus/DESIGN.md`](../bonus/DESIGN.md): thiết kế flywheel dữ liệu cho chatbot
CSKH SaaS tiếng Việt, gồm kiến trúc, năm quyết định có trade-off và phương án bị loại.

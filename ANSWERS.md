# ANSWERS.md — Báo Cáo Phân Tích Kỹ Thuật & Hồ Sơ Nghiệm Thu Lab 28

**Dự án:** Day 28 Track 2 — Platform Integration & Production Readiness  
**Học viên / Nhóm thực hiện:** Lương Hoàng Minh  
**Mã nguồn repo:** [TRACK2_Day28_2A202601490_LuongHoangMinh](https://github.com/hoangminh37/TRACK2_Day28_2A202601490_LuongHoangMinh.git)  
**Ngày hoàn thành:** 03/09/2026  

---

## 1. Sơ Đồ Kiến Trúc & Phân Định Quyền Sở Hữu (Architecture & Ownership)

Hệ thống được tổ chức thành 5 tầng kiến trúc kết nối qua 10 điểm ranh giới (IP01 → IP10):

![Sơ đồ kiến trúc nền tảng RAG Lab 28](docs/images/lab28-architecture-overview.png)

### Bảng phân định quyền sở hữu (Ownership Matrix):
| Tầng (Layer) | Thành phần | Điểm kết nối (IP) | Đội ngũ phụ trách (Owner) | Vai trò chính |
|---|---|:---:|---|---|
| **L1 Compute & Serving** | Envoy Gateway | IP08 | `team-platform` | Reverse proxy, route request, gán `x-request-id`, Rate limit Token Bucket. |
| | FastAPI Serving | IP01, IP07 | `team-serving` | Ingestion endpoint, orchestration truy hồi RAG, phân loại mức sẵn sàng. |
| | vLLM Engine | IP07 | `team-serving` | OpenAI-compatible LLM inference server với metrics `vllm:`. |
| **L2 Data Ingestion & Storage** | Kafka Cluster | IP01, IP02 | `team-ingestion` | Buffer dữ liệu bất đồng bộ qua các topic `data.raw`, `data.processed`, `data.raw.dlq`. |
| | Airflow 3 Pipeline | IP02 | `team-ingestion` | Điều phối DAG `lab28_ingestion_pipeline`, quản lý lifecycle và asset events. |
| | Spark & Delta Lake | IP03 | `team-data` | Spark Connect thực hiện Delta MERGE đảm bảo tính toàn vẹn và time travel. |
| | Qdrant Vector DB | IP05 | `team-serving` | Vector store lưu trữ hybrid embeddings với ID deterministic từ `doc_id`. |
| **L3 Feature & Model Registry** | Feast Feature Store | IP04 | `team-data` | Quản lý feature view `asker_activity_v1`, phục vụ online feature lookup. |
| | MLflow Registry | IP06 | `team-data` | Quản lý model release, metadata, git sha, delta version và alias `champion`. |
| **L4 Observability & Ops** | Prometheus & Grafana | IP09 | `team-platform` | Thu thập metric toàn bộ dịch vụ, hiển thị Golden Signals và alert. |
| | OpenTelemetry / Jaeger | IP10 | `team-platform` | Distributed tracing xuyên suốt W3C traceparent từ Client đến Lakehouse. |

---

## 2. Kết Quả Fast Suite & Tính Toàn Vẹn (Fast Suite Output)

Toàn bộ các bộ kiểm thử tĩnh và kiểm thử đơn vị đều đạt 100%:

```text
1. Unit tests (starter-tests + tests):
   uv run pytest starter-tests tests -q
   ===> 87 passed in 25.53s

2. Linter & Formatting:
   uv run ruff check .
   ===> All checks passed!

3. Ma trận tích hợp 10 IP:
   uv run python scripts/verify_matrix.py
   ===> OK 245 checks passed: contracts/integration-matrix.yaml matches the repository

4. Tính tương thích đa nền tảng (OS portability):
   uv run python scripts/check_portability.py
   ===> OK supported workflow is host-path and shell independent

5. Kubernetes & GitOps Manifests:
   uv run python scripts/validate_manifests.py
   ===> Kubernetes and GitOps manifest contracts passed

6. Integration Tests (5 Journeys J1 -> J5):
   uv run pytest integration-tests -m "not gpu and not langsmith" -q
   ===> 56 passed, 16 deselected in 120.34s (0:02:00)
```

---

## 3. Trade-offs (Các đánh đổi thiết kế hệ thống)

Trong quá trình thiết kế và kết nối 10 ranh giới (IP01 → IP10), hệ thống đưa ra 4 quyết định đánh đổi kỹ thuật cốt lõi:

### 3.1. Kafka At-Least-Once Delivery kết hợp Idempotent Delta MERGE vs. Two-Phase Commit (2PC)
- **Bối cảnh:** Khi gửi dữ liệu ingestion từ Gateway vào Kafka và đổ vào Lakehouse, việc đảm bảo "không mất dữ liệu và không trùng dữ liệu" là bài toán sống còn.
- **Lựa chọn:** Thay vì dùng giao thức phân tán Two-Phase Commit (2PC) hoặc Kafka Transactions nặng nề (gây độ trễ cao, giảm throughput và dễ deadlock khi network partition), hệ thống chọn mô hình **At-least-once delivery + Idempotent consumer**.
- **Cơ chế:**
  - Producer gửi bản tin kèm `idempotency-key` và W3C `traceparent` qua Kafka header.
  - Phía consumer (Spark/Delta), hàm `dedupe_latest()` lọc trùng lặp theo `idempotency_key` và chỉ giữ bản ghi có cặp `(occurred_at, event_id)` mới nhất.
  - Lệnh Spark Delta `MERGE INTO` đảm bảo ghi đè (update) nếu khóa đã tồn tại và chỉ chèn (insert) nếu khóa mới.
- **Đánh đổi:** Tốn thêm một lượng nhỏ CPU/RAM ở worker để deduplicate và chạy câu lệnh MERGE thay vì Append thuần túy, nhưng đổi lại hệ thống có khả năng chịu lỗi cực cao (Fault-tolerant), Kafka replay thoải mái mà không bao giờ sinh dữ liệu rác hay trùng lặp.

### 3.2. Phân tách Offline/Online Feature Store (Feast) vs. Truy vấn trực tiếp Lakehouse
- **Bối cảnh:** Request path cần thông tin hành vi của người hỏi (`asker_activity_v1`) để đưa vào context grounding.
- **Lựa chọn:** Tách rời hai luồng:
  - **Offline path:** Spark đọc Delta Lake định kỳ tạo snapshot tổng hợp các chỉ số (`feedback_count`, `avg_rating`, `negative_ratio`, `delta_version`).
  - **Online path:** Feast materialize snapshot này vào Online Store (KV store / Redis / SQLite) phục vụ truy vấn qua HTTP `/get-online-features`.
- **Đánh đổi:** Chấp nhận độ trễ dữ liệu (Data Freshness trễ khoảng vài giây đến vài phút tùy chu kỳ chạy DAG Airflow) để đổi lấy độ trễ truy vấn cực thấp (`lookup_ms ~ 1.5ms`), hoàn toàn cách ly tải phục vụ suy luận (serving path) khỏi tải phân tích dữ liệu nặng nề trên Lakehouse.

### 3.3. Phân cấp mức sẵn sàng (Readiness Semantics: `ready`, `degraded`, `not_ready`)
- **Bối cảnh:** Khi một thành phần trong chuỗi suy luận gặp trục trặc, chính sách y tế hệ thống cần quyết định có đánh sập pod (HTTP 503) hay không.
- **Lựa chọn:**
  - Lỗi thành phần bắt buộc (`mandatory=True`: Kafka, MLflow champion, Qdrant): Trả về `not_ready` (HTTP 503), Gateway gỡ pod khỏi rotation để bảo vệ tính đúng đắn.
  - Lỗi thành phần tùy chọn (`mandatory=False`: Feast online cache lạnh, vLLM ở môi trường cơ bản): Trả về `degraded` (HTTP 200).
- **Đánh đổi:** Khi ở chế độ degraded, câu trả lời suy luận có thể thiếu context mở rộng nhưng hệ thống vẫn duy trì tính khả dụng (High Availability), tránh hiện tượng "thác đổ lỗi" (Cascading failure) khiến toàn bộ nền tảng sập chỉ vì một tính năng phụ tạm thời gián đoạn.

### 3.4. Envoy Local Rate Limiting bảo vệ Downstream
- **Bối cảnh:** Client hoặc các luồng batch gửi dữ liệu ồ ạt qua Gateway `:8080`.
- **Lựa chọn:** Cấu hình Envoy Token Bucket với `max_tokens: 10`, `tokens_per_fill: 10`, `fill_interval: 1s`.
- **Đánh đổi:** Chấp nhận trả về mã `429 Too Many Requests` cho các client bắn tải vượt ngưỡng, đảm bảo API FastAPI và broker Kafka phía sau không bao giờ bị quá tải hàng đợi (Buffer exhaustion) hay sập OOM.

---

## 4. Happy-Path Trace & Thông Tin Phiên Bản (Provenance)

Hành trình kiểm thử chuẩn (Golden Path - J1) đã thực hiện thành công một chu trình khép kín:
- **Trace ID W3C xuyên suốt:** `e78078feecee401b9c64af2684fddfa0`
  - Đã xuất hiện đồng nhất tại: Header HTTP Gateway ➔ Kafka Record Header `data.raw` ➔ Airflow DAG Context ➔ Spark Delta Merge ➔ OpenTelemetry Collector.
- **Airflow DAG Run ID:** `it-87fb6321` (DAG `lab28_ingestion_pipeline`).
  - Cả 4 tasks đều hoàn thành `success` ở lần thử đầu tiên (`try_number: 1`):
    1. `drain_kafka_into_delta`
    2. `refresh_online_features`
    3. `index_new_documents`
    4. `announce_processed_batch`
  - 4 Asset events được công bố: `lab28://delta/feedback`, `lab28://delta/documents`, `lab28://qdrant/lab28_documents`, `lab28://feast/asker_activity`.
- **Delta Lake Table Version:**
  - Bảng `feedback`: Version 3 (và tăng lên v8 sau toàn bộ suite kiểm thử).
  - Bảng `documents`: Version 5 (chứa 17 documents).
- **MLflow Model Registry:**
  - Tên mô hình: `lab28-rag-release`
  - Phiên bản được chọn (`champion` alias): **Version 1**
  - MLflow Run ID: `74b1e115755c4ba281e9d4cc06fe98ff`

---

## 5. Bằng Chứng Phục Hồi & Không Mất Dữ Liệu (Failure & Recovery)

Hệ thống đã chứng minh độ bền bỉ qua 2 bài test khắt khe:
1. **Kiểm thử Kafka Replay & Idempotency (`IT-J2`):**
   - Giả lập tình huống mạng chập chờn khiến Kafka gửi lại cùng một mẻ dữ liệu (duplicate batch).
   - Nhờ cơ chế `dedupe_latest()` và Delta MERGE theo `idempotency_key`, số dòng trong bảng Delta và số vector points trong Qdrant không hề bị tăng lên hay sai lệch.
2. **Kiểm thử Sự Cố Phục Hồi Không Mất Dữ Liệu (`IT-J4`):**
   - Dừng đột ngột container Feast và Qdrant trong lúc gửi dữ liệu.
   - Hệ thống chuyển mượt mà sang trạng thái `degraded` (phục vụ fallback mà không chết cứng).
   - Khi service được bật lại, Airflow pipeline tái thử nghiệm và commit bù toàn bộ batch dữ liệu, đảm bảo **No Data Loss (0% mất mát dữ liệu)**.

---

## 6. Load Testing & Phân Tích Điểm Nghẽn (Bottleneck Analysis)

Thực nghiệm đo tải được tiến hành bằng lệnh:
```bash
uv run python load-tests/run_profile.py --requests 200 --workers 8
```

### 6.1. Dữ liệu thực nghiệm
- **Tổng số request:** 200 requests.
- **Độ tương tranh (Concurrency):** 8 workers chạy song song.
- **Kết quả phản hồi (Status codes):**
  - `200 OK`: 14 requests.
  - `0 (Bị ngắt do HTTP 429)`: 186 requests.
- **Độ trễ phản hồi (Latency Percentiles):**
  - **P50:** 1.51 ms
  - **P95:** 245.54 ms
  - **P99:** 337.27 ms

### 6.2. Phân tích điểm nghẽn
1. **Điểm nghẽn cố ý tại Gateway (Envoy Rate Limit):**
   - 186 requests nhận mã trạng thái 429 (`local_rate_limited`). Điều này chứng minh thuật toán Token Bucket của Envoy hoạt động cực kỳ chính xác: 14 request đầu tiên vét cạn bucket và token được fill trong giây đầu, toàn bộ các request burst còn lại bị chặn đứng ngay tại ranh giới ngoài cùng của hệ thống mà không chạm tới downstream.
2. **Độ trễ ấn tượng tại P50 (1.51 ms):**
   - Đối với các request được chấp nhận hoặc bị từ chối sớm tại gateway, độ trễ chỉ xấp xỉ 1.5ms. Điều này cho thấy tầng gateway C++ của Envoy hoạt động cực kỳ nhẹ và tối ưu.
3. **Đề xuất nâng cấp cho Production:**
   - Khi triển khai production với lượng người dùng lớn, cấu hình rate limit cần chuyển từ **Local Rate Limit** (theo từng pod Gateway) sang **Global Rate Limit Service (RLS)** kết hợp cụm Redis tập trung.
   - Bổ sung cấu hình Horizontal Pod Autoscaler (HPA) cho `lab28-api` dựa trên metric `envoy_http_downstream_rq_total`.

---

## 7. Kubernetes / GitOps & Cơ Chế Rollback (Validation & Drift Evidence)

### 7.1. Kết quả kiểm tra Manifest K8s
Lệnh kiểm tra: `uv run python scripts/validate_manifests.py` đạt chuẩn tuyệt đối:
- **Tài nguyên Kubernetes khai báo:** `Deployment`, `Service`, `ServiceAccount`, `ConfigMap`, `HorizontalPodAutoscaler`, `PodDisruptionBudget`, `NetworkPolicy`, `Gateway`, `HTTPRoute`.
- **Bảo mật Pod:** `runAsNonRoot: true`, cấm hoàn toàn tag `:latest` (bắt buộc dùng immutable pinned tags), bắt buộc đầy đủ `readinessProbe`, `livenessProbe`, `resources` limits/requests và `securityContext`.
- **Gateway API v1:** Toàn bộ Route và Listener tuân thủ chuẩn `gateway.networking.k8s.io/v1`.

### 7.2. GitOps & Cơ chế Tự Phục Hồi (Self-healing & Rollback)
- **Argo CD Application:** Khai báo tại `gitops/application.yaml` trỏ vào `targetRevision: v3.0.0` (pinned release, không dùng nhánh động `main`/`HEAD`).
- **Cơ chế Rollback Model:** Sử dụng MLflow alias (`champion`):
  - Khi cần rollback model, không cần redeploy code hay build lại image container; chỉ cần chuyển alias `champion` về version cũ (Version 1).
  - API tự động resolve release mới nhất mang alias `champion` theo cơ chế cache TTL.
- **Cơ chế Rollback Infrastructure:**
  - Nếu xảy ra trôi dạt cấu hình (Configuration Drift) trên cụm K8s, Argo CD tự động phát hiện out-of-sync và đồng bộ lại về trạng thái mong muốn (Desired State) được định nghĩa trong Git.
  - Rollback chỉ đơn giản là lệnh `git revert` đưa commit hash về phiên bản trước.

---

## 8. Khoảng Cách Đến Môi Trường Production Thực Tế (Production Gaps)

| Thành phần | Hiện trạng bài Lab | Yêu cầu chuẩn Production |
|---|---|---|
| **Bảo mật & Secrets** | Dùng `.env` và password file cục bộ | Sử dụng HashiCorp Vault / AWS Secrets Manager; bật mTLS giữa các container; chứng thực OAuth2/OIDC/JWT tại Gateway. |
| **Lưu trữ Lakehouse** | Mount thư mục local `.lab28/delta` qua volume | Sử dụng Cloud Object Storage (Amazon S3 / Google Cloud Storage / MinIO) với cơ chế đồng bộ phân tán đa vùng (Multi-region replication). |
| **Orchestration** | Airflow standalone chạy LocalExecutor | Airflow cụm chạy KubernetesExecutor / CeleryExecutor; kích hoạt DAG tự động theo sự kiện Kafka (KEDA Event-driven autoscaling). |
| **Hạ tầng GPU Suy Luận** | Chạy chế độ fallback / degraded mode cục bộ | Triển khai cụm vLLM phân tán trên Kubernetes với Ray cluster, hỗ trợ vLLM Continuous Batching, Tensor Parallelism và PagedAttention. |
| **Giám sát & Cảnh báo** | Prometheus scrape cục bộ và Jaeger UI | Tích hợp hệ thống phân tích tập trung (Grafana Cloud / Datadog / OpenSearch), thiết lập Alertmanager gửi cảnh báo PagerDuty/Slack theo SLO/SLA. |

---

## 9. Bảng Phân Chia Vai Trò & Đóng Góp (Team Roles & Contributions)

Thực hiện toàn diện 5 vai trò kiến trúc:

- **Ingestion & Orchestration (IP01, IP02):** Hoàn thiện `event_headers()` cho Kafka (truyền chuẩn W3C `traceparent` và `idempotency-key`), kiểm soát Airflow 3 DAG, quản lý topic Kafka (`data.raw`, `data.processed`, `data.raw.dlq`).
- **Data & ML (IP03, IP04, IP06):** Hoàn thiện `dedupe_latest()` đảm bảo tính idempotent của Delta MERGE, cấu hình Feast online request theo hợp đồng `asker_activity_v1`, kiểm tra model tracking và champion alias trên MLflow.
- **Serving & Retrieval (IP05, IP07):** Lập chỉ mục vector Qdrant với UUID deterministic (`stable_point_id`), thiết lập client kết nối vLLM và logic degraded mode.
- **Platform & Observability (IP08, IP09, IP10):** Cấu hình Envoy rate-limit, xác thực K8s/GitOps manifests, kiểm tra Prometheus targets, Grafana dashboard và theo dõi trace xuyên suốt hệ thống qua OpenTelemetry.
- **Presenter & SRE:** Hoàn thiện bộ bằng chứng 10 integration points tại `evidence/`, biên soạn `ANSWERS.md` và chuẩn bị kịch bản demo sự cố.

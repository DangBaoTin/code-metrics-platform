# 13 - Bao Cao Hoc Thuat (Tieng Viet)

## Tieu de

Nen tang Du Lieu Code Metrics: Kien truc Lambda phan tan cho telemetry giao duc thoi gian thuc va phan tich theo lo

## Tom tat

De tai xay dung mot nen tang data engineering dau-cuoi cho bai toan telemetry trong moi truong hoc lap trinh. He thong su dung Kafka de ingest su kien toc do cao, Spark Structured Streaming cho xu ly thoi gian thuc, Spark Batch cho ETL va mo hinh hoa, Cassandra de luu tru medallion (Bronze/Silver/Gold), MongoDB de luu metadata theo mo hinh replica set, Airflow de dieu phoi lich ETL hang ngay, va Streamlit de phuc vu dashboard giam sat + BI/AI. Thiet ke huong den tinh ben vung van hanh trong moi truong local cluster (3 node Cassandra + 3 node MongoDB).

## 1. Dat van de

Nen tang hoc lap trinh phat sinh lien tuc du lieu nop bai, trang thai chay, va chi so he thong. Nhu cau dat ra gom:

1. Giam sat thoi gian thuc (leaderboard, canh bao do tre, nghi ngo gian lan).
2. Tong hop theo ngay cho bao cao BI.
3. Tao dau ra Data Science (du doan roi bo, goi y bai tiep theo).
4. Dam bao kha nang mo rong va chong loi trong boi canh phan tan.

## 2. Kien truc tong the

- Ingestion layer: Kafka (`raw_submissions`, `system_metrics`)
- Speed layer: Spark Streaming
- Batch layer: Spark SQL + Spark MLlib
- Storage:
  - Cassandra ring 3 node (masterless)
  - MongoDB replica set 3 node (Primary-Secondary)
- Orchestration: Airflow
- Serving: Streamlit dashboard

Mo hinh xu ly: Lambda architecture ket hop medallion architecture.

## 3. Thiet ke du lieu

### 3.1 Cassandra

- Bronze: luu payload raw de tranh mat du lieu
- Silver: submission da parse/canonical
- Gold: bang tong hop phuc vu dashboard va mo hinh

Cac bang Gold chinh:

- `gold_live_leaderboard`
- `gold_system_alerts`
- `gold_engagement_scores`
- `gold_difficulty_heatmap`
- `gold_subscription_revenue`
- `gold_instructor_report_card`
- `gold_dropout_predictions`
- `gold_next_problem_recommendations`
- `gold_adaptive_difficulty_profiles`

### 3.2 MongoDB

- `users`, `problems`, `transactions`, `ratings`
- Dung de enrich va tinh toan cac chi so kinh doanh/hoc tap

## 4. Trien khai tinh phan tan va HA

### 4.1 Cassandra masterless ring

- 3 node ngang hang, khong co master co dinh
- Replication factor 3 trong keyspace local
- Co the tiep tuc phuc vu khi 1 node loi (tuy thuoc consistency level)

### 4.2 MongoDB Primary-Secondary

- 1 PRIMARY + 2 SECONDARY
- Tu dong failover qua replica set
- Cai dat khoi tao replica set tu service `mongodb-rs-init`

## 5. Giai thich code theo lop

### 5.1 Streaming (`src/code_metrics/processing/stream_leaderboard.py`)

- Readiness check Cassandra truoc khi start
- Reset checkpoint + truncate bang Gold khi can
- Doc 2 topic Kafka va ghi Bronze/Gold bang foreachBatch
- Retry theo dong va theo lo cho Cassandra write
- Rule canh bao:
  - HIGH_LATENCY khi execution > 5s
  - CHEAT_DETECTED theo cua so 30s + da IP

### 5.2 Batch ETL (`src/code_metrics/processing/batch_etl.py`)

- Parse Bronze -> Silver
- Feature engineering theo user
- BI: engagement, heatmap, revenue, instructor report
- AI: Logistic Regression dropout + ALS recommendation + adaptive profile

### 5.3 Dashboard (`src/code_metrics/dashboard/app.py`)

- Cac view real-time + batch + AI
- Tu dong refresh va hien trang thai ket noi DB
- Enrich thong tin nguoi dung/bai toan tu MongoDB

### 5.4 Simulator (`src/code_metrics/simulator/generate_logs.py`)

- Tao su kien nop bai + metric he thong
- Co retry ket noi Kafka
- Co fallback user/problem pool khi Mongo khong dap ung
- Co latency spike de kich hoat canh bao > 5s

## 6. Van hanh va on dinh he thong

Suot qua trinh tich hop, he thong gap va da xu ly:

- Loi Cassandra warm-up/race condition
- Loi checkpoint offset mismatch
- Loi simulator khong bat duoc Kafka ngay dau
- Loi reset checkpoint tren macOS (ENOTEMPTY)

Giai phap da ap dung:

- Retry/readiness co cau hinh
- `failOnDataLoss=false` cho stream local
- Session reuse cho Cassandra write
- Retry-safe checkpoint delete

## 7. Ket qua

He thong dat duoc:

1. Ingest va xu ly telemetry thoi gian thuc.
2. Dashboard leaderboard/alerts cap nhat theo dong su kien.
3. Batch ETL tao day du bang BI/AI theo yeu cau de bai.
4. Co tai lieu van hanh va troubleshooting day du.

## 8. Huong mo rong

- Benchmark throughput/latency co so lieu dinh luong
- Bo sung schema contract cho event
- Nang cap deployment profile len cloud managed services
- Theo doi mo hinh va data quality pipeline

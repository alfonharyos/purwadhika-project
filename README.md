# Purwadhika Data Engineering Portfolio Projects
End-to-End Data Pipeline — Batch, Streaming, Scraping, and Analytics

---

## Overview

Repositori ini berisi 5 proyek **end-to-end data pipeline** yang dikembangkan untuk menunjukkan keahlian teknis pada Apache **Airflow**, **dbt**, **BigQuery**, **GCS**, **PostgreSQL**, **Pub/Sub**, dan **Dataflow**.
Setiap proyek merepresentasikan kasus industri nyata, mencakup **batch analytics**, **real-time streaming**, **scraping**, dan **text extraction**.

---

## Projects Summary

|   | Project | Domain Focus | Pipeline Type | Stack Utama |
| - | ------- | ------------ | ------------- | ----------- |
| 1 | 🎬 [Movie Streaming — Batch User Analytics](./documentation/movie_streaming/movie_streaming_analytics.md) | OTT Platform, Retention, Revenue | Batch | Airflow, dbt, PostgreSQL, BigQuery |
| 2 | ⚡ [Movie Streaming — Real-Time Video Playback Alerts](./documentation/movie_streaming/movie_streaming_playback_monitoring.md) | Streaming Analytics, Monitoring | Streaming | Pub/Sub, Dataflow, BigQuery |
| 3 | 🚕 [Weekend Night Trips — NYC Taxi Mobility Analysis](./documentation/taxi_trip/taxi_trip.md) | Urban Mobility, Consulting | Batch Incremental | Airflow, dbt, GCS, BigQuery |
| 4 | 📊 [Pinjol Daily Statistics Scraper & Reporter](./documentation/pinjol/pinjol_stats.md) | Fintech, Monitoring | Web Scraping + Daily ETL | Selenium, Airflow, BigQuery, Discord |
| 5 | 🧾 [Putusan Mahkamah Agung (MA) — Analisis Kasus ITE](./documentation/putusan_ma/putusan_ma.md) | LegalTech, NLP | Scraping + Text Extraction | Airflow, GCS, BigQuery, PyMuPDF, LexRank |

---

## Tools & Framework Integration

| Layer | Teknologi | Deskripsi |
| ----- | --------- | --------- |
| **Orchestration** | Apache Airflow | Menjadwalkan dan memonitor alur ETL/ELT |
| **Data Warehouse** | Google BigQuery | Penyimpanan utama untuk analisis dan dashboard |
| **Transformation** | dbt Core | Modelling SQL berbasis dependency & incremental |
| **Streaming** | Pub/Sub + Dataflow | Real-time ingestion & filtering pipeline |
| **Storage** | Google Cloud Storage | Intermediate & archive layer |
| **Database (OLTP)** | PostgreSQL | Simulasi transaksi (user, subscription, payments) |
| **Scraping & Extraction** | Selenium, BeautifulSoup, PyMuPDF | Mengambil data publik & mengekstrak teks hukum |
| **Notification** | Discord Webhook | Monitoring status job otomatis |
| **Containerization** | Docker Compose | Deploy lokal multi-service |
| **NLP / Summarization** | NLTK, LexRank | Ekstraksi & ringkasan teks hukum |

---

## 🧱 Repository Structure
```
purwadhika-project
├── dags
│   ├── dag_init_movie_streaming_data_to_postgres.py
│   ├── dag_init_movie_streaming_data_postgres_to_bigquery.py
│   ├── dag_init_dbt_movie_streaming_data.py
│   ├── dag_ingest_movie_streaming_data_to_postgres_hourly.py
│   ├── dag_ingest_movie_streaming_data_postgres_to_bigquery_daily.py
│   ├── dag_dbt_movie_streaming_data_monthly.py
│   ├── dag_start_subscriber_alert_video_playback_events_bigquery.py
│   ├── dag_start_subscriber_alert_video_playback_events_discord.py
|   |
│   ├── dag_init_dbt_taxi_trip_bq_monthly.py
│   ├── dag_ingest_taxi_trip_to_gcs_bq_monthly.py
│   ├── dag_ingest_taxi_zone.py
│   ├── dag_dbt_taxi_trip_bq_monthly.py
|   |
│   ├── dag_ingest_and_report_scrape_pinjol_daily.py
|   |
│   ├── dag_scraping_putusan_ma_bq_gcs_monthly.py
|   |
│   └── dag_del_datase.py
│
├── data
│   ├── data_init_movie_streaming
│   │   ├── movies.csv
│   │   ├── payments.csv
│   │   ├── ratings.csv
│   │   ├── subscriptions.csv
│   │   ├── users.csv
│   │   └── watch_sessions.csv
│   └── tmp/
│
├── dbt
│   ├── dbt_alfon_project
│   │   ├── analyses
│   │   ├── dbt_project.yml
│   │   ├── dbt_packages
│   │   ├── logs
│   │   ├── macros
│   │   └── models
│   │       ├── movie_streaming
│   │       │   ├── dbt_packages
│   │       │   ├── mart
│   │       │   │   ├── mart_movie_popularity_monthly.sql
│   │       │   │   ├── mart_movie_rating_monthly.sql
│   │       │   │   ├── mart_schema.yml
│   │       │   │   ├── mart_subscription_revenue_monthly.sql
│   │       │   │   └── mart_user_activity_monthly.sql
│   │       │   │
│   │       │   ├── model
│   │       │   │   ├── dim_date.sql
│   │       │   │   ├── dim_movies.sql
│   │       │   │   ├── dim_subscriptions.sql
│   │       │   │   ├── dim_users.sql
│   │       │   │   ├── fact_movie_popularity.sql
│   │       │   │   ├── fact_payments.sql
│   │       │   │   ├── fact_ratings.sql
│   │       │   │   ├── fact_user_activity.sql
│   │       │   │   └── model_schema.yml
│   │       │   │
│   │       │   └── preparation
│   │       │       ├── prep_movies.sql
│   │       │       ├── prep_payments.sql
│   │       │       ├── prep_ratings.sql
│   │       │       ├── prep_schema.yml
│   │       │       ├── prep_subscriptions.sql
│   │       │       ├── prep_users.sql
│   │       │       └── prep_watch_sessions.sql
│   │       │
│   │       └── taxi_trip
│   │           ├── mart
│   │           │   ├── mart_schema.yml
│   │           │   ├── mart_taxi_trip_holiday_daily.sql
│   │           │   ├── mart_taxi_trip_holiday_summary.sql
│   │           │   └── mart_taxi_trip_holiday_zone_flow.sql
│   │           │
│   │           ├── model
│   │           │   ├── dim_holiday.sql
│   │           │   ├── dim_location.sql
│   │           │   ├── dim_payment_type.sql
│   │           │   ├── fact_taxi_trip.sql
│   │           │   └── model_schema.yml
│   │           │
│   │           └── preparation
│   │               ├── prep_schema.yml
│   │               └── prep_taxi_trip.sql
│   │
│   └── dbt_profiles
│        └── profiles.yml
│
├── docker
│   ├── Dockerfile.airflow
│   ├── Dockerfile.postgresql
│   └── Dockerfile.publisher
│
├── envs
│   ├── .env.movie_streaming
│   └── .env.sender
│
├── helpers
│   ├── bigquery_helper.py
│   ├── dataframe_helper.py
│   ├── discord_helper.py
│   ├── email_helper.py
│   ├── gcs_helper.py
│   ├── image_helper.py
│   ├── logger.py
│   ├── postgresql_helper.py
│   ├── pubsub_helper.py
│   └── requests_helper.py
│
├── nltk_data
│
├── pgdata
│
├── schemas
│   ├── bigquery_schemas
│   └── postgres_schemas
│
├── scripts
│   ├── docker-entrypoint-init
│   │   ├── airflow_init
│   │   │   └── airflow_init.sh
│   │   └── postgres_init
│   │       └── postgres_init.sh
│   │
│   ├── mahkamah_agung
│   │   ├── crawl_putusan_ma.py
│   │   └── extract_putusan_pdf.py
│   │
│   ├── movie_streaming
│   │   ├── movie_streaming_data_generator.py
│   │   ├── publish_message_video_playback_events_movie_streaming.py
│   │   ├── subscriber_alert_bigquery_video_playback_events.py
│   │   └── subscriber_alert_discord_video_playback_events.py
│   │
│   ├── pinjol
│   │   └── crawl_pinjol_statistics.py
│   │
│   └── taxi_trip
│       └── script_ingest_taxi_trip.py
│   
├── secrets
│   └── bigquery-key.json
│
├── .env
├── docker-compose.yml
├── requirements.txt
```
---

## 📁 Folder Explanation
| Folder | Deskripsi | Dipakai di |
| ------ | --------- | ---------- |
| **`dags/`** | Berisi semua Airflow DAG per proyek (`dag_ingest_*.py`, `dag_dbt_*.py`). | Semua |
| **`scripts/`** | Script Python utama (extract, publish, subscriber, scraping). | Semua |
| **`helpers/`** | Modul Python bersama: `bigquery_helper.py`, `discord_helper.py`, `gcs_helper.py`, `requests_helper.py`, `dataframe_helper.py`. | Semua |
| **`schemas/`** | Definisi schema BigQuery (`*_schema.py` / .sql) untuk otomatisasi pembuatan tabel. | Movie, Taxi, Pinjol, MA |
| **`dbt/`** | Folder proyek dbt (`dbt_alfon_project`) mencakup model prep, fact, dim, mart. | Movie, Taxi |
| **`data/tmp/`** | Temporary lokal untuk download PDF sebelum di-upload ke GCS. | Putusan MA |
| **`secrets/`** | Menyimpan file kredensial GCP (misal `bigquery-key.json`). | Semua |
| **`nltk_data/`** | Resource tambahan untuk ekstraksi teks (LexRank, tokenizer). | Putusan MA |
| **`.env`** | Konfigurasi environment variable untuk pipeline (BQ, GCS, Discord, Pub/Sub). | Semua |

---

## 🐳 Docker Compose
File docker-compose.yml mengorkestrasi seluruh container lintas proyek.

| Service | Fungsi | Port | Catatan |
| ------- | ------ | ---- | ------- |
| **postgres** | Database sumber simulasi untuk Movie Streaming (OLTP) | `5432` | Data transaksi pengguna, pembayaran, dan aktivitas nonton |
| **adminer** | Web UI untuk PostgreSQL | `8081` | Akses manual database untuk debugging |
| **selenium** | Chromium container untuk scraping Pinjol | `4444` | Digunakan oleh `crawl_pinjol_statistics.py` |
| **airflow-init**      | Menyiapkan schema, folder, dan connection awal Airflow | - | Menjalankan `airflow_init.sh` satu kali di startup |
| **airflow-webserver** | UI utama untuk mengelola DAG | `8080` | Jalankan & monitor pipeline |
| **airflow-scheduler** | Menjalankan task DAG sesuai jadwal | - | Berjalan otomatis setelah init sukses |
| **publisher** | Publisher event real-time Movie Streaming | - | Mengirim event ke Pub/Sub secara periodik |


---

## 🔔 Monitoring & Alerting

Semua pipeline dilengkapi `Discord notification`

- ✅ Success
- ⚠️ Warning
- ❌ Failure (error message & timestamp)

---

## 📘 Individual Project READMEs
- [🎬 Movie Streaming (Batch)](./documentation/movie_streaming/movie_streaming_analytics.md)
- ⚡ [Movie Streaming (Streaming)](./documentation/movie_streaming/movie_streaming_playback_monitoring.md)
- 🚕 [Weekend Night Trips — NYC Taxi](./documentation/taxi_trip/taxi_trip.md)
- 📊 [Pinjol Daily Statistics Scraper](./documentation/pinjol/pinjol_stats.md)
- 🧾 [Putusan Mahkamah Agung (MA)](./documentation/putusan_ma/putusan_ma.md)

---
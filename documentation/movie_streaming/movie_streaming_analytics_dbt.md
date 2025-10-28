# dbt Project — Movie Streaming Analytics

Pipeline ini merupakan bagian transformasi batch dari proyek **🎬 Movie Streaming Data Pipeline — Batch User Analytics**.  
Tujuan utama layer dbt ini adalah untuk:

1. Menyusun data dari `raw` (hasil Airflow ingestion dari PostgreSQL) menjadi **tabel analitik siap pakai** di BigQuery.  
2. Menyediakan **struktur dimensional (dim/fact)** dan **mart agregat bulanan** untuk analisis churn, revenue, dan engagement.  
3. Mengimplementasikan **incremental merge strategy** agar efisien saat update bulanan.

Semua transformasi dijalankan dengan perintah:
```py
bash_command=(
    f"cd {DBT_PROJECT_PATH} "
    f"&& dbt run --select movie_streaming,tag:{tag} "
    f"--profiles-dir {DBT_PROFILES_PATH} --target {target}"
)
```
tag = layer ("preparation", "model", "mart")        
target = dataset location ("us")

---

## 🗂️ Folder Structure
```py
dbt
├───dbt_alfon_project
|   ├── macros/
│   │   └── generate_schema_name.sql
│   │
|   ├── models/
│   │   └── movie_streaming/
│   │       |   
│   │       |   # Clean & deduplicate raw data
│   │       ├── preparation/   
│   │       |     
│   │       |   # Dimensional (dim/fact)   
│   │       ├── model/            
│   │       |   
│   │       |   # Aggregated analytics marts
│   │       └── mart/             
│   │
│   └── dbt_project.yml
│
└── dbt_profiles
    └── profiles.yml
```

---

## Project Schema & Table Structure
Setiap layer memiliki schema berbeda di BigQuery:
| Layer | Schema | Table | Tujuan |
|---------|---------|-----------|--------|
| Raw (source) | jcdeol004_alfon_movie_streaming_raw | `raw_users`, `raw_movies`, `raw_payments`, `raw_subscriptions`, `raw_ratings`, `raw_watch_sessions` | Hasil ingestion Airflow dari PostgreSQL |
| Preparation | jcdeol004_alfon_movie_streaming_preparation | `prep_users`, `prep_movies`, `prep_payments`, `prep_subscriptions`, `prep_ratings`, `prep_watch_sessions` | Cleaning, validasi, deduplikasi |
| Model (dim/fact) | jcdeol004_alfon_movie_streaming_model | `dim_users`, `dim_movies`, `dim_date`, `dim_subscriptions`, `fact_movie_popularity`, `fact_payments`, `fact_ratings`, `fact_user_activity` | Star schema untuk analisis |
| Mart | jcdeol004_alfon_movie_streaming_mart | `mart_movie_popularity_monthly`, `mart_movie_rating_monthly`, `mart_subscription_revenue_monthly`, `mart_user_activity_monthly` | Agregasi bulanan untuk dashboard |

---

## Layer Breakdown
### Preparation Layer
Folder: `./dbt/dbt_alfon_project/models/movie_streaming/preparation`

Tujuan:
- Membersihkan data mentah dari layer raw.
- Menghapus `duplikasi` record (QUALIFY `ROW_NUMBER()`).
- Menormalkan kolom teks dan format tanggal.
- Menyimpan `run_date_bq` sebagai audit timestamp dan sebagai referensi `incremental merge`.

materialized:
- table: `prep_users`, `prep_movies`
- incremental: `prep_subscriptions`, `prep_watch_sessions`, `prep_payments`, `prep_ratings`

contoh `prep_payments.sql`
```sql
{{ config(
    materialized='incremental',
    unique_key='payment_id',
    incremental_strategy='merge', 
    tags=['preparation']
) }}

with base as (
    select
        payment_id,
        user_id,
        subscription_id,
        amount,
        payment_date,
        method,
        status,
        created_at,
        run_date_bq
    from {{ source('jcdeol004_alfon_movie_streaming_raw', 'raw_payments') }}
    {% if is_incremental() %}
        where run_date_bq > (select max(run_date_bq) from {{ this }})
    {% endif %}
)

select
    payment_id,
    user_id,
    subscription_id,
    amount,
    payment_date,
    method,
    status,
    created_at,
    run_date_bq
from base
qualify row_number() over (partition by payment_id order by created_at desc) = 1
```

---

### Model Layer (Dimensional Model)
Folder: `./dbt/dbt_alfon_project/models/movie_streaming/model`

Tujuan:
- Menyusun dimension table (dim_) dan fact table (fact_)
- Menetapkan struktur star schema
- Memanfaatkan incremental merge untuk fact table       

#### Dimension      
Semua model dimension menggunakan `materialized='table'`
| Table | Deskripsi | key |
|-------|-----------|-----|
| dim_users | Data user dengan subscription_id aktif | user_id |
| dim_movies | Metadata film | movie_id |
| dim_subscriptions | Informasi paket langganan | subscription_id |
| dim_date | Kalender waktu untuk join ke fact | date_key |

#### Fact
Semua model fact menggunakan `materialized='incremental'` dengan strategi `MERGE`
| Table | Deskripsi | Unique key |
|-------|-----------|------------|
| fact_user_activity | Aktivitas tonton per sesi | session_id
| fact_movie_popularity | Popularitas film harian | movie_id_date |
| fact_payments | Transaksi pembayaran user | payment_id |
| fact_ratings | Review & rating user | rating_id |

Contoh `fact_user_activity.sql`
```sql
{{ config(
    materialized='incremental',
    unique_key='session_id',
    incremental_strategy='merge'
) }}

with base as (
    select
        session_id,
        user_id,
        movie_id,
        start_time,
        end_time,
        TIMESTAMP_DIFF(end_time, start_time, MINUTE) as watch_duration_minutes,
        case when completion_rate >= 0.85 then true else false end as completed_flag,
        device_type,
        date(start_time) as date_key,
        run_date_bq,
        row_number() over (partition by session_id order by created_at desc) as rn
    from {{ source('jcdeol004_alfon_movie_streaming_preparation', 'prep_watch_sessions') }}
    {% if is_incremental() %}
        where date(start_time) > (select max(date_key) from {{ this }})
    {% endif %}
)
select * from base where rn = 1
```

---

### Mart Layer (Analytical Aggregates)
Folder: `./dbt/dbt_alfon_project/models/movie_streaming/mart`

Tujuan:
- Menyediakan **data agregasi bulanan** untuk analisis bisnis dan dashboard.
- Mengukur **popularitas konten, rating film, engagement user, serta performa pendapatan & churn.**
- Menyimpan **historical summary (full history)** dengan strategi **incremental merge** agar efisien dan dapat diperbarui bulanan tanpa overwrite total.

Semua model fact menggunakan `materialized='incremental'` dengan strategi `MERGE` *(Merge last 2 months only)*
```sql
{% if is_incremental() %}
    where date_trunc(date_key, month) >= date_sub(current_date, interval 2 month)
{% endif %}
```

| Model | Fokus Analitik | Input Source | Unique Key | Partition | Cluster | Output |
|--------|----------------|---------------|-------------|------------|--------|--------|
| `mart_movie_popularity_monthly` | Popularitas film per bulan | `fact_movie_popularity` | `movie_month_key` | `month_key` | `movie_id` | Agregasi jumlah penonton, waktu tonton |
| `mart_movie_rating_monthly` | Rating & engagement film per bulan | `fact_ratings` | `movie_month_key` | `month_key` |`movie_id` | Skor rata-rata, jumlah rating |
| `mart_user_activity_monthly` | Aktivitas user & durasi tonton bulanan | `fact_user_activity` | `user_month_key` | `month_key` | `subscription_id` | Durasi tonton, sesi, completion rate |
| `mart_subscription_revenue_monthly` | Pendapatan & churn langganan per bulan | `fact_payments` | `subscription_month_key` | `month_key` | `user_id` | Revenue, active/new/churn users |


---
**1️⃣ mart_movie_popularity_monthly.sql**     

Tujuan:     
Menghitung popularitas film bulanan berdasarkan aktivitas menonton (jumlah penonton, durasi tonton, rata-rata durasi per sesi).

```sql
select
  movie_id,
  date_trunc(date_key, month) as month_key,
  sum(total_watch_count) as total_watch_count,
  sum(unique_viewers) as unique_viewers,
  avg(avg_watch_duration) as avg_watch_duration,
  sum(total_watch_time) as total_watch_time,
  concat(cast(movie_id as string), '_', format_date('%Y%m', month_key)) as movie_month_key,
  max(run_date_bq) as run_date_bq
from {{ source('jcdeol004_alfon_movie_streaming_model', 'fact_movie_popularity') }}
group by movie_id, month_key
```

**Output Insight:**     
Film paling sering ditonton tiap bulan
Genre atau tipe konten dengan engagement tertinggi

---
**2️⃣ mart_movie_rating_monthly.sql**     

Tujuan:     
Menghitung rata-rata rating film bulanan, serta jumlah dan keragaman user yang memberikan rating.

```sql
select
  movie_id,
  date_trunc(date_key, month) as month_key,
  avg(rating_score) as avg_rating,
  count(rating_id) as total_ratings,
  count(distinct user_id) as unique_raters,
  concat(cast(movie_id as string), '_', format_date('%Y%m', month_key)) as movie_month_key,
  max(run_date_bq) as run_date_bq
from {{ source('jcdeol004_alfon_movie_streaming_model', 'fact_ratings') }}
group by movie_id, month_key
```

**Output Insight:**     
- Film dengan rating tertinggi vs engagement rendah
- Perbandingan rating antar bulan untuk deteksi tren kualitas konten

---
**3️⃣ mart_user_activity_monthly.sql**                
Tujuan:  

Menganalisis tingkat aktivitas pengguna bulanan, termasuk durasi total, jumlah sesi, dan tingkat completion.

```sql
select
  user_id,
  date_trunc(date_key, month) as month_key,
  sum(watch_duration_minutes) as total_watch_time_minutes,
  count(session_id) as total_sessions,
  avg(watch_duration_minutes) as avg_watch_duration,
  sum(case when completed_flag then 1 else 0 end) as completed_sessions,
  concat(cast(user_id as string), '_', format_date('%Y%m', month_key)) as user_month_key,
  max(run_date_bq) as run_date_bq
from {{ source('jcdeol004_alfon_movie_streaming_model', 'fact_user_activity') }}
group by user_id, month_key
```

**Output Insight:**     
- Jumlah user aktif tiap bulan
- Durasi rata-rata nonton per user
- Completion rate per user (indikasi loyalitas konten)

---
**4️⃣ mart_subscription_revenue_monthly.sql**     
Tujuan:     
Mengukur performansi pendapatan langganan, termasuk jumlah pembayaran sukses, user aktif baru, dan churn rate bulanan.

```sql
with revenue as (
  select
    subscription_id,
    date_trunc(date(payment_date), month) as month_key,
    sum(amount) as total_amount,
    count(payment_id) as total_payments,
    sum(case when status='completed' then 1 else 0 end) as status_completed,
    sum(case when status='failed' then 1 else 0 end) as status_failed,
    concat(cast(subscription_id as string), '_', format_date('%Y%m', month_key)) as subscription_month_key,
    max(run_date_bq) as run_date_bq
  from {{ source('jcdeol004_alfon_movie_streaming_model', 'fact_payments') }}
  group by subscription_id, month_key
),
user_activity as (
  select
    month_key,
    count(distinct subscription_id) as total_active_users,
    count(distinct case when first_payment_date = month_key then subscription_id end) as new_users,
    count(distinct case when last_payment_date < month_key then subscription_id end) as churned_users
  from (
    select subscription_id, month_key,
           min(month_key) over (partition by subscription_id) as first_payment_date,
           max(month_key) over (partition by subscription_id) as last_payment_date
    from revenue
  )
  group by month_key
)
select * from revenue r
left join user_activity ua on r.month_key = ua.month_key
```

**Output Insight:**     
- Tren pendapatan bulanan & pertumbuhan user aktif
- Rasio new users vs churned users (churn indicator)
- Prediksi performa retention untuk bulan berikutnya

---
## Data Testing
Testing dalam pipeline dbt Movie Streaming:

- **Integrity Tests** — `not_null`, `unique`
- **Relationship Tests** — memastikan foreign key valid antar tabel
- **Business Logic Validation** — konsistensi dimensi & fakta
- **Partition Metadata** — agar efisien saat query di BigQuery

---

### Layer Testing Coverage
| Layer | Schema | Fokus Utama | Testing Scope |
|--------|---------|--------------|----------------|
| **Preparation** | `jcdeol004_alfon_movie_streaming_preparation` | Validasi hasil cleansing dari raw | Uniqueness, Foreign Key, Null Check |
| **Model (Dim & Fact)** | `jcdeol004_alfon_movie_streaming_model` | Star Schema consistency | Primary Key, Relationships, Partition Metadata |
| **Mart (Analytics)** | `jcdeol004_alfon_movie_streaming_mart` | Data agregat & historis | Key Uniqueness, Completeness per Month |

---

### Testing Rules - Preparation Layer
| Table | Key Columns | Relationship | Tests |
|--------|-------------|---------------|-------|
| `prep_users` | `user_id` | — | `unique`, `not_null` |
| `prep_movies` | `movie_id` | — | `unique`, `not_null`, `release_year not_null` |
| `prep_subscriptions` | `subscription_id` | → `prep_users.user_id` | `unique`, `relationships` |
| `prep_watch_sessions` | `session_id` | → `prep_users.user_id`, `prep_movies.movie_id` | `unique`, `relationships` |
| `prep_payments` | `payment_id` | → `prep_users.user_id`, `prep_subscriptions.subscription_id` | `unique`, `relationships` |
| `prep_ratings` | `rating_id` | → `prep_users.user_id`, `prep_movies.movie_id` | `unique`, `relationships` |

---

### Testing Rules - Dimensional Tables
| Table | Key | Tests | Notes |
|--------|-----|--------|-------|
| `dim_users` | `user_id` | `not_null`, `unique` | Email unique & country not null |
| `dim_movies` | `movie_id` | `not_null`, `unique` | Film harus punya `release_year` |
| `dim_subscriptions` | `subscription_id` | `not_null`, `unique` | `plan_type` dan `status` wajib diisi |
| `dim_date` | `date_key` | `not_null`, `unique` | Basis waktu referensi |

---

### Testing Rules - Fact Tables
| Table | Key | Relationships | Notes |
|--------|-----|----------------|-------|
| `fact_user_activity` | `session_id` | → `dim_users.user_id`, `dim_movies.movie_id` | Basis semua analisis engagement |
| `fact_movie_popularity` | — | → `dim_movies.movie_id` | Hasil agregasi dari user activity |
| `fact_payments` | `payment_id` | → `dim_users.user_id`, `dim_subscriptions.subscription_id` | Sumber utama revenue |
| `fact_ratings` | `rating_id` | → `dim_users.user_id`, `dim_movies.movie_id` | Basis analisis rating & review |

---

### Testing Rules - Mart Layer
| Table | Key | Test | Notes |
|-------|-----|------|-------|
| `mart_movie_popularity_monthly` | `movie_month_key` | `unique`, `not_null` | Popularitas film per bulan
| `mart_movie_rating_monthly` | `movie_month_key` | `unique`, `not_null` | Rata-rata rating film per bulan |
| `mart_user_activity_monthly` | `user_month_key` | `unique`, `not_null` | Aktivitas user per bulan |
| `mart_subscription_revenue_monthly` | `subscription_month_key` | `unique`, `not_null` | Revenue dan churn bulanan |

---

## Macros
`generate_schema_name.sql`

Digunakan agar schema output `..._preparation`, `..._model`, `..._mart` otomatis sesuai dengan konfigurasi dbt_project.yml

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}
```

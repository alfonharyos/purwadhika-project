# 🧾 Putusan Mahkamah Agung (MA) — Analisis Kasus ITE

Proyek ini bertujuan `mengumpulkan` dan `mengekstrak` **putusan pengadilan** kasus **ITE** (Informasi dan Transaksi Elektronik) dari **website Mahkamah Agung (MA)** untuk analisis pola seperti:

- **Pola pelanggaran ITE**: UU yang paling sering diterapkan
- **Sebaran geografis**: kota/pengadilan dengan volume kasus tinggi
- **Profil terdakwa**: pekerjaan, usia, jenis kelamin, kewarganegaraan
- **Tren tahunan**: kenaikan/penurunan kasus per tahun

serta membangun pipeline yang `tahan terhadap` *`error`* website, *`PDF besar`*, dan *`struktur dokumen tidak seragam`*.

---

## Latar Belakang

Website `putusan3.mahkamahagung.go.id` berisi ribuan putusan dari seluruh Indonesia.

| Tantangan | Penjelasan |
| --------- | ---------- |
| ⚙️ Tidak ada API resmi | Data hanya bisa diakses lewat HTML page |
| 🕒 Website lambat dan sering tidak stabil | Perlu mekanisme retry dan batch scraping |
| 📄 File besar & tidak seragam | PDF bisa mencapai 100–300 halaman dan memiliki struktur berbeda |
| 🔁 Pertumbuhan bulanan | Kasus baru muncul setiap bulan, perlu update otomatis |

Masalah-masalah ini diatasi dengan membangun **pipeline otomatis bulanan** berbasis Airflow + GCS + BigQuery, yang memiliki mekanisme `retry`, `validasi`, dan `penyimpanan sementara` (temporary local storage) agar scraping tetap stabil dan idempotent.

---

## Fitur Ketahanan Pipeline (Resilience Features)
Pipeline ini didesain menjawab permasalahan.

Beberapa fitur kunci yang membuat pipeline ini Tahan:

| Fitur | Deskripsi | Nilai Tambah |
| ----- | --------- | ------------ |
| 🔁 `max_retries` & `timeout` | Setiap request HTML memiliki retry hingga 5x dan batas waktu (default 120 detik). | Menghindari kegagalan akibat koneksi lambat atau halaman error. |
| 🧩 Validasi jumlah putusan (`max_putusan`) | Sistem memeriksa total putusan per bulan dari halaman utama sebelum scraping per halaman. | Menghindari scraping berlebih dan memastikan hasil lengkap. |
| 📂 Temporary Local Storage (`/data/tmp`) | File PDF disimpan sementara di disk lokal sebelum diunggah ke GCS. | Mencegah kehilangan file akibat koneksi upload terputus dan menjaga idempotensi proses. |
| ☁️ Upload GCS terjadwal (batch per bulan) | Semua file PDF bulan tersebut diupload sekaligus setelah selesai scraping. | Mengurangi request dan meningkatkan kecepatan pipeline. |
| 🧱 Merge incremental ke BigQuery (stg → raw) | Data staging selalu digabung (merge) ke tabel raw secara idempotent. | Tidak ada duplikasi, mudah untuk audit atau rerun. |
| 📡 Discord Alert Integration | Mengirim pesan sukses/error ke channel Discord otomatis. | Monitoring real-time dan alert ketika scraping gagal. |
| 📄 Ekstraksi PDF adaptif | Algoritma regex dan LexRank bekerja bahkan untuk PDF tidak seragam. | Data tetap dapat diekstrak meskipun layout berubah. |

---

## Structure
```
putusan_ma
├── dags
│   └── dag_scraping_putusan_ma_bq_gcs_monthly.py
│
├── data
│   └── tmp                    # Temporary local storage (PDF download)
│
├── docker
│   └── Dockerfile.airflow
│
├── helpers                    # Utilitas umum untuk semua proyek
│   ├── bigquery_helper.py
│   ├── dataframe_helper.py
│   ├── discord_helper.py
│   ├── gcs_helper.py
│   └── requests_helper.py
│
├── nltk_data                  # Resource NLTK untuk ekstraksi teks
│
├── schemas
│   └── bigquery_schemas
│       └── putusan_ma_schema.py
│
├── scripts
│   ├── docker-entrypoint-init
│   │   └── airflow_init
│   │       └── airflow_init.sh
│   │
│   ├── mahkamah_agung
│       ├── crawl_putusan_ma.py       # Scraper halaman & link putusan
│       └── extract_putusan_pdf.py    # Ekstraksi data dari PDF
│
├── secrets
│   └── bigquery-key.json
│
├── .env
├── docker-compose.yml
└── requirements.txt
```

--- 

## Architecture Overview
```mathematica
Scraping Link Putusan
        ↓
Scraping Page Detail Putusan
        ↓
Download PDF ke Local (/data/tmp/)
        ↓
Validasi & Upload PDF ke GCS
        ↓
Ekstraksi Isi PDF (Local → BigQuery)
        ↓
Upload hasil ekstraksi ke GCS
        ↓
Merge ke BigQuery (stg → raw)
```
---

## Pipeline Detail
Scraping Link Putusan → Scraping Detail Halaman → Ekstraksi Isi PDF → Merge ke Tabel Raw

### 1️⃣ Scraping Link Putusan
- Mengambil daftar **link** per bulan (`no_putusan`, `tgl_putusan`, `link_page_putusan`)
- **Validasi** jumlah target (`max_putusan`)
- **Retry** dan timeout per halaman

### 2️⃣ Scraping Detail Halaman
- Mengambil **metadata utama**:     
    Hakim, Klasifikasi, Amar, Kata Kunci, Tanggal Register
- **Download** file PDF ke folder lokal:        
    `./data/tmp/{no_putusan}.pdf`
- Setelah lolos **validasi** (file ada dan berisi teks),        
    file diunggah ke Google Cloud Storage (GCS).
- **Link GCS** PDF disimpan ke `BigQuery` pada kolom:
    `gcs_pdf_path`

### 3️⃣ Ekstraksi Isi PDF (Local → BigQuery)
- Membuka file PDF dari `/data/tmp/`
- Bersihkan teks dari header/footer website MA
    - Ekstraksi informasi hukum:
    - Nomor putusan
    - Biodata terdakwa (nama, usia, pekerjaan, agama, dll)
    - Barang bukti
    - Rujukan hukum (pasal, UU)
    - Amar putusan
    - Ringkasan hukum otomatis (*LexRank*)
- Simpan **hasil ekstraksi** ke `BigQuery` (`stg_putusan_extract_pdf`)
- Upload hasil file (**PDF & ekstraksi**) ke `GCS` untuk arsip
- Menambahkan kolom `gcs_extract_pdf_path` untuk **Link GCS** hasil ekstract

gcs_extract_pdf_path
### 4️⃣ Merge ke Tabel Raw
- Data **staging** digabung (**merge**) ke dataset `jcdeol004_alfon_putusan_ma_raw`
- Menjamin konsistensi antar rerun

---

## Dataset Bigquery
| Dataset | Tabel | Fungsi |
| ------- | ----- | ------ |
| `jcdeol004_alfon_putusan_ma_stg` | `stg_putusan_link_page` | Link dan tanggal putusan |
| | `stg_putusan_ma` | Detail metadata tiap putusan |
| | `stg_putusan_extract_pdf` | Hasil ekstraksi teks hukum |
| `jcdeol004_alfon_putusan_ma_raw` | `raw_putusan_ma` | Merge akhir semua metadata
| | `raw_putusan_extract_pdf` | Merge akhir ekstraksi PDF |

---

## Tech Stack
| Layer | Tools/Tech |
| ----- | ---------- |
| Orkestrasi | Apache Airflow |
| Penyimpanan Cloud | Google Cloud Storage (GCS) |
| Data Warehouse | BigQuery |
| Scraping | BeautifulSoup + Requests |
| PDF Parsing | PyMuPDF (fitz) |
| NLP & Ringkasan | Sumy LexRankSummarizer |
| Monitoring | Discord Webhook + Airflow Logs |
| Containerization | Docker Compose |
---

## Rencana Lanjutan
- 🔍 **Integrasi** dbt BigQuery untuk membangun layer analisis (prep, model, mart)
- 🧠 **Fine-tuning** model LLM Indonesia untuk abstraksi putusan hukum otomatis
- 📊 **Dashboard** di Looker Studio atau Metabase
- ⏱️ **Optimasi** scraping paralel + caching halaman stabil



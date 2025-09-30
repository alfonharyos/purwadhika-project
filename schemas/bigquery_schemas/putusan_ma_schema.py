from google.cloud import bigquery


PUTUSAN_LINK_PAGE_SCHEMA=[
    bigquery.SchemaField("no_putusan", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("tgl_putusan", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("link_page_putusan", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("run_date", "DATE")
]


PUTUSAN_MA_SCHEMA=[
    bigquery.SchemaField("no_putusan", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("tgl_putusan", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("nama_terdakwa", "STRING"),
    bigquery.SchemaField("link_page_putusan", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("link_pdf_putusan", "STRING"),
    bigquery.SchemaField("gcs_pdf_path", "STRING"),
    bigquery.SchemaField("tingkat_proses", "STRING"),
    bigquery.SchemaField("klasifikasi", "STRING"),
    bigquery.SchemaField("kata_kunci", "STRING"),
    bigquery.SchemaField("tahun", "INT64"),
    bigquery.SchemaField("tgl_register", "DATE"),
    bigquery.SchemaField("lembaga_pengadilan", "STRING"),
    bigquery.SchemaField("hakim_ketua", "STRING"),
    bigquery.SchemaField("hakim_anggota", "STRING"),
    bigquery.SchemaField("panitera", "STRING"),
    bigquery.SchemaField("amar", "STRING"),
    bigquery.SchemaField("amar_lainnya", "STRING"),
    bigquery.SchemaField("catatan_amar", "STRING"),
    bigquery.SchemaField("tgl_musyawarah", "DATE"),
    bigquery.SchemaField("tgl_dibacakan", "DATE"),
    bigquery.SchemaField("run_date", "DATE")
]


PUTUSAN_EXTRACT_PDF_SCHEMA=[
    bigquery.SchemaField("no_putusan", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("tgl_putusan", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("gcs_extract_pdf_path", "STRING"),
    bigquery.SchemaField("summary_extractive_short", "STRING"),
    bigquery.SchemaField("nama_terdakwa", "STRING"),
    bigquery.SchemaField("tempat_lahir_terdakwa", "STRING"),
    bigquery.SchemaField("tanggal_lahir_terdakwa", "DATE"),
    bigquery.SchemaField("jenis_kelamin_terdakwa", "STRING"),
    bigquery.SchemaField("kebangsaan_terdakwa", "STRING"),
    bigquery.SchemaField("tempat_tinggal_terdakwa", "STRING"),
    bigquery.SchemaField("agama_terdakwa", "STRING"),
    bigquery.SchemaField("pekerjaan_terdakwa", "STRING"),
    bigquery.SchemaField("barang_bukti", "STRING"),
    bigquery.SchemaField("rujukan_hukum", "STRING"),
    bigquery.SchemaField("amar_putusan", "STRING"),
    bigquery.SchemaField("run_date", "DATE")
]

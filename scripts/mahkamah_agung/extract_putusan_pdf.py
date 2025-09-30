import fitz
import re
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lex_rank import LexRankSummarizer

from helpers.discord_helper import discord_send_message


# ------------ Convert PDF to Text -----------------
def pdf_to_text(pdf_path):
    doc = fitz.open(pdf_path)
    all_text = []
    for i, page in enumerate(doc):
        text = page.get_text("text")
        if text.strip():
            all_text.append(text)
    return "\n".join(all_text)


# ------------ Summary Text Using LexRank ----------
def legal_summary(text, n_sentences=20):
    parser = PlaintextParser.from_string(text, Tokenizer("english"))
    summarizer = LexRankSummarizer()
    summary = summarizer(parser.document, n_sentences)
    summary_text = " ".join(str(sentence) for sentence in summary)

    # filter untuk kalimat penting hukum
    keywords = ["putusan", "terdakwa", "hukuman", "pidana", "perdata", "amar"]
    filtered = [s for s in summary_text.split(". ") if any(k in s.lower() for k in keywords)]
    return "\n".join(filtered) if filtered else summary_text


# --------------- Clean Header/Footer -------------
def filter_header_footer_putusan(text: str) -> str:
    text = re.sub(r"^(.*?putusan\.mahkamahagung\.go\.id\n)", "", text,
                  flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"Disclaimer.*?021-384 3348 \(ext\.318\)", "", text,
                  flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"Halaman\s+\d+", "", text)
    text = re.sub(r"\n{2,}", "\n", text).strip()
    return text

# ---------- Extract Nomor Putusan -----------------
def extract_no_putusan(text):
    text = text[:500] # 500 char awal
    pattern = r"Nomor\s*:?\s*([^\n\r]+)"
    match = re.search(pattern, text, re.IGNORECASE)
    return match.group(1).strip()


# ---------- Extract Data Terdakwa -----------------
# ======= STEP 1: Normalize =======
def normalize_biodata_block(text: str) -> str:
    clean = re.sub(r"[ \t]+", " ", text)
    clean = re.sub(r"\n+", " ", clean)
    clean = clean.lower()
    # clean = re.sub(r"(\b[a-z](?:\s+[a-z])+\b)", 
    #                lambda m: m.group(1).replace(" ", ""), clean)
    return clean

# ======= STEP 2: Label tagging =======
def mark_biodata_labels(text: str) -> str:
    labels = {
        "nama_lengkap": r"\bnama(\s+lengkap)?\b",
        "tempat_lahir": r"tempat\s+lahir",
        "umur/tanggal_lahir": r"umur\s*/?\s*(tgl\.?|tanggal)\s+lahir",
        "jenis_kelamin": r"jenis\s+kelamin",
        "kebangsaan": r"kebangsaan(\s*[/\n]\s*kewarganegaraan)?|kewarganegaraan",
        "tempat_tinggal": r"tempat\s+tinggal|alamat",
        "agama": r"agama",
        "pekerjaan": r"pekerjaan",
        "pendidikan": r"pendidikan", 
    }
    for key, pattern in labels.items():
        text = re.sub(pattern, f" @@{key}@@ ", text, flags=re.IGNORECASE)
    return text

# ======= STEP 3: Cleaning value =======
def clean_value(val: str) -> str:
    val = val.strip(" ;\n\t.:")
    val = re.sub(r"\s+", " ", val)
    val = re.sub(r"(;|\s|\.)+\b([0-2]?\d|30)\b\s*$", "", val)  # hapus angka penomoran
    return val.strip()

# ======= STEP 4: Parsing =======
def parse_biodata(text: str) -> dict:
    marked = mark_biodata_labels(normalize_biodata_block(text))

    stopwords_block = ["penyidik", "jaksa", "menimbang", "mengadili", "amar", "mengingat", "terdakwa"]
    stop_pattern = "|".join(stopwords_block)

    stopwords_value = ["berdasarkan", "pengadilan", "terdakwa", "penyidik"]

    data = {}
    matches = re.findall(r"@@(.*?)@@(.*?)(?=@@|$)", marked, flags=re.DOTALL)

    for label, value in matches:
        label = label.strip()
        if not label:
            continue

        # stopwords blok
        value = re.split(stop_pattern, value, flags=re.IGNORECASE)[0]
        # stop di label lain
        value = re.split(r"@@.*?@@", value)[0]
        # stop di kata pengganggu
        for sw in stopwords_value:
            value = re.split(rf"\b{sw}\b", value, flags=re.IGNORECASE)[0]

        val = clean_value(value)

        if val:
            if label == "nama_lengkap":
                data[label] = val.title()
            else:
                data[label] = val

    return data

# ======= STEP 5: Tanggal Lahir =======
# mapping bulan Indonesia ke angka
BULAN_MAP = {
    "januari": 1, "februari": 2, "maret": 3, "april": 4, "mei": 5, "juni": 6,
    "juli": 7, "agustus": 8, "september": 9, "oktober": 10, "november": 11, "desember": 12
}

def extract_tanggal_lahir(val: str):
    """
    Ambil hanya tanggal lahir dari field 'umur/tanggal lahir'
    Contoh: '44 tahun/10 Januari 1980' -> '1980-01-10'
    """
    if not val:
        return None
    
    # cari pola "dd bulan yyyy"
    m = re.search(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", val, flags=re.IGNORECASE)
    if m:
        day, bulan, tahun = m.groups()
        bulan = bulan.lower()
        if bulan in BULAN_MAP:
            try:
                date_obj = datetime(int(tahun), BULAN_MAP[bulan], int(day))
                return date_obj.date().isoformat()
            except ValueError:
                return None
    return None

# ======= STEP 6: Pipeline =======
def extract_data_terdakwa(text: str) -> dict:

    # 1. Batasi hanya 1000-1500 char pertama, karena biodata biasanya di awal
    text = text[:1500]

    # 2. Parse biodata
    data = parse_biodata(text)

    # 3. Ambil tanggal lahir
    if "umur/tanggal_lahir" in data:
        tgl = extract_tanggal_lahir(data["umur/tanggal_lahir"])
        if tgl:
            data["tanggal_lahir"] = tgl
        data.pop("umur/tanggal_lahir", None)

    return data


# ---------- Extract Semua Rujukan (Pasal, UU) -----------------
def extract_all_rujukan(text):
    # Beberapa pola umum rujukan hukum
    patterns = [
        r"Pasal\s+\d+[A-Za-z]*\s*(?:ayat\s*\(\d+\))?\s*(?:huruf\s*[a-z])?(?:\s+[^\.;,\n]*)?",   # Pasal 303 ayat (1) KUHP
        r"Undang-Undang\s+Nomor\s+\d+\s+Tahun\s+\d+(?:\s+[^\.;,\n]*)?",                        # UU No 31 Tahun 1999
        r"KUHP\s+Pasal\s+\d+[A-Za-z]*",                                                        # KUHP Pasal 378
        r"KUHAP\s+Pasal\s+\d+[A-Za-z]*",                                                       # KUHAP Pasal 183
        r"jo\.\s*Undang-Undang\s+Nomor\s+\d+\s+Tahun\s+\d+(?:\s+[^\.;,\n]*)?",                 # jo. UU
    ]

    results = []
    for p in patterns:
        matches = re.findall(p, text, flags=re.IGNORECASE)
        results.extend([" ".join(m.split()) for m in matches])  # rapikan spasi

    # hapus duplikat tapi tetap urut
    seen, unique = set(), []
    for r in results:
        if r not in seen:
            seen.add(r)
            unique.append(r)

    return unique

# ---------- Extract amar Putusan (MENGADILI) -----------------
def extract_mengadili(text):
    # regex untuk MENGADILI atau M E N G A D I L I (pakai \s* di tiap huruf)
    pattern = r"M\s*E\s*N\s*G\s*A\s*D\s*I\s*L\s*I\s*:"

    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        start = match.end()
        # ambil teks setelah MENGADILI sampai stopword berikut
        stopwords = ["\nDemikianlah diputuskan"]
        stop_pattern = "|".join(stopwords)
        after = re.split(stop_pattern, text[start:], flags=re.IGNORECASE)[0]
        return after.strip()
    return None

# ---------- Extract Barang Bukti -----------------
def extract_barang_bukti(text):
    # Stopwords akhir bagian barang bukti
    stopwords = ["Dirampas", "Dikembalikan", "Dimusnahkan", "Menjatuhkan", "Mengadili", "Amar"]
    stop_pattern = "|".join(stopwords)

    # Regex cari blok barang bukti
    amar_pattern = rf"barang bukti berupa[: ](.+?)(?=\n(?:{stop_pattern})|\Z)"
    match = re.search(amar_pattern, text, re.IGNORECASE | re.DOTALL)
    if match:
        block = match.group(1)
        # Rapikan whitespace
        block = re.sub(r"\s+", " ", block).strip()
        return block
    return None


# ==============================================================
# main fuction pdf file -> txt and DataFrame
def extract_data_putusan_pdf(pdf_path, gcs_extract_pdf_path=None, n_sentences=2, logger=None):
    logger.info(f'Extracting {pdf_path} ....')

    # 1. Ambil teks
    raw_text = pdf_to_text(pdf_path)

    # 2. Bersihkan teks
    clean_text = filter_header_footer_putusan(raw_text)

    # 3. Ekstraksi data
    no_putusan = extract_no_putusan(clean_text)
    biodata = extract_data_terdakwa(clean_text)
    barang_bukti = extract_barang_bukti(clean_text)
    rujukan = extract_all_rujukan(clean_text)
    amar = extract_mengadili(clean_text)
    summary_short = legal_summary(clean_text, n_sentences=n_sentences)

    # 4. Map biodata ke schema BigQuery
    data = {
        "no_putusan": no_putusan,
        "gcs_extract_pdf_path": gcs_extract_pdf_path,
        "summary_extractive_short": summary_short,
        "nama_terdakwa": biodata.get("nama_lengkap"),
        "tempat_lahir_terdakwa": biodata.get("tempat_lahir"),
        "tanggal_lahir_terdakwa": biodata.get("tanggal_lahir"),
        "jenis_kelamin_terdakwa": biodata.get("jenis_kelamin"),
        "kebangsaan_terdakwa": biodata.get("kebangsaan"),
        "tempat_tinggal_terdakwa": biodata.get("tempat_tinggal"),
        "agama_terdakwa": biodata.get("agama"),
        "pekerjaan_terdakwa": biodata.get("pekerjaan"),
        "barang_bukti": barang_bukti,
        "rujukan_hukum": ", ".join(rujukan) if rujukan else None,
        "amar_putusan": amar,
        "run_date": datetime.now(ZoneInfo("Asia/Jakarta")).date(),
    }

    if data['nama_terdakwa'] == None:
        logger.error(f'Extraction Failed {pdf_path}')
        raise
    else:
        return clean_text, pd.DataFrame([data])
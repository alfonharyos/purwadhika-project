import re
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from datetime import date
from helpers.requests_helper import fetch_html_requests

MAP_MONTH = {
    1: 'Januari',
    2: 'Februari',
    3: 'Maret',
    4: 'April',
    5: 'Mei',
    6: 'Juni',
    7: 'Juli',
    8: 'Agustus',
    9: 'September',
    10: 'Oktober',
    11: 'Nopember',
    12: 'Desember'
}
# reverse map: bulan -> angka
MONTH_TO_NUM = {v: k for k, v in MAP_MONTH.items()}

def covert_tanggal_id_to_date(date_str: str) -> date:
    """
    Convert string tanggal format Indonesia (contoh: '13 Januari 2025')
    menjadi datetime.date
    """
    parts = date_str.strip().split()
    if len(parts) != 3:
        raise ValueError(f"Format tidak dikenali: {date_str}")
    
    day = int(parts[0])
    month_str = parts[1]
    year = int(parts[2])
    
    month = MONTH_TO_NUM.get(month_str)
    if not month:
        raise ValueError(f"Bulan tidak dikenali: {month_str}")
    
    return date(year, month, day)

def parse_putusan_list(html: str, target_month: str , logger):
    soup = BeautifulSoup(html, "html.parser")
    records = []

    for idx, spost in enumerate(soup.select("div.spost.clearfix"), start=1):
        try:
            # ambil tanggal putusan
            tgl_putus = None
            for sm in spost.find_all("div", class_="small"):
                m = re.search(r"Putus\s*:\s*(\d{2}-\d{2}-\d{4})", sm.get_text(" ", strip=True))
                if m:
                    try:
                        tgl_putus = datetime.strptime(m.group(1), "%d-%m-%Y").date()
                    except ValueError as ve:
                        logger.warning(f"[Record {idx}] Invalid date format: {m.group(1)} -> {ve}")
                    break

            # ambil link + nomor putusan
            a_tag = spost.find("a", href=re.compile(r"/direktori/putusan/"))
            no_putus, link = None, None
            if a_tag:
                link = a_tag.get("href", "").strip()
                text = a_tag.get_text(strip=True)
                m = re.search(r"Nomor\s+(.+)", text)
                no_putus = m.group(1).strip() if m else text

            # skip incomplete
            if not (no_putus and tgl_putus and link):
                logger.debug(f"[Record {idx}] Skipped incomplete data")
                continue

            # filter bulan
            if target_month and tgl_putus.strftime("%Y-%m") != target_month:
                continue

            records.append({
                "no_putusan": no_putus,
                "tgl_putusan": tgl_putus,
                "link_page_putusan": link,
            })

        except Exception as e:
            logger.error(f"[Record {idx}] Unexpected parsing error: {e}", exc_info=True)
            continue

    return pd.DataFrame(records)

def total_no_putusan(html: str, month: int, logger):
    """
    Extract total number of putusan from HTML by month number (1-12).
    Returns integer count (0 if not found).
    """

    target_month = MAP_MONTH.get(month)
    if not target_month:
        logger.error(f"Invalid month: {month}")
        raise ValueError(f"Invalid month number: {month}")

    soup = BeautifulSoup(html, "html.parser")

    # cari semua p.card-text
    for p_tag in soup.select("p.card-text"):
        text = p_tag.get_text(" ", strip=True)
        if target_month in text:
            span = p_tag.find("span", class_="badge badge-secondary")
            if span and span.get_text(strip=True).isdigit():
                return int(span.get_text(strip=True))

    # kalau tidak ketemu
    logger.warning(f"No total putusan found for {target_month}")
    return 0

# ========================================================
# ------- list Putusan Nomor | Tanggal | Link Page -------
# ========================================================
def scrape_putusan_links(
    year: int, month: int, logger,
    kategori: str, pengadilan: str,
    max_retries=5, timeout=120
):
    target_month = f"{year}-{month:02d}"
    pengadilan = f'pengadilan/{pengadilan}/'
    kategori = f'kategori/{kategori}-1/'
    all_rows = []
    page = 1
    max_putusan = 1
    total_scraped = 0

    while total_scraped < max_putusan:
        url = f"https://putusan3.mahkamahagung.go.id/direktori/index/{pengadilan}{kategori}/tahunjenis/putus/tahun/{year}/page/{page}.html"
        try:
            html = fetch_html_requests(url, logger, timeout=timeout, max_retries=max_retries)
        except Exception as e:
            logger.error(f"[Page {page}] Failed to fetch HTML: {e}", exc_info=True)
            raise

        try:
            df_page = parse_putusan_list(html, target_month, logger)
        except Exception as e:
            logger.error(f"[Page {page}] Failed to parse HTML: {e}", exc_info=True)
            df_page = pd.DataFrame()

        try:
            max_putusan = total_no_putusan(html, month, logger)
        except Exception as e:
            logger.warning(f"[Page {page}] Could not extract total putusan: {e}")

        # kalau halaman kosong tetap lanjut cari di hlm berikutnya
        if not df_page.empty:
            all_rows.append(df_page)
            total_scraped += len(df_page)

            logger.info(f"[Page {page}] Collected {len(df_page)} records (total so far: {total_scraped}/{max_putusan})")
        else:
            logger.info(f"[Page {page}] No records for target month {target_month}, skipping.")

        page += 1

        # safety guard -> jika belum ketemu
        if page > 200:
            logger.error("Page limit reached (200), stopping to avoid infinite loop.")
            break

    if all_rows:
        return pd.concat(all_rows, ignore_index=True)

    return pd.DataFrame(columns=["no_putusan", "date_putusan", "link_page_putusan"])


# ========================================================
# ----------------- Detail Putusan Page ------------------
# ========================================================
def scrape_detail_putusan(url: str, logger=None, max_retries=5, timeout=120) -> pd.DataFrame:
    """
    Scrape detail putusan page -> return DataFrame with one row.
    """
    html = fetch_html_requests(url=url, logger=logger, max_retries=max_retries, timeout=timeout)
    soup = BeautifulSoup(html, "html.parser")
    
    # get nama terdakwa
    try:
        span = soup.find_all("span", id="title_pihak")
        match = re.search(r"Terdakwa:<br/?>\s*([^<]+)", str(span), flags=re.IGNORECASE)
        nama_terdakwa = match.group(1).strip() if match else None
    except Exception:
        nama_terdakwa = None
    
    # get pdf url
    try:
        pdf_div = soup.find("div", class_="card-body bg-white")
        pdf_url = pdf_div.find_all("a")[1]["href"]
    except Exception:
        pdf_url = "no_pdf_file"
    
    # cols mapping (HTML → schema BQ)
    col_map = {
        'Nomor': 'no_putusan',
        'Tingkat Proses': 'tingkat_proses',
        'Klasifikasi': 'klasifikasi',
        'Kata Kunci': 'kata_kunci',
        'Tahun': 'tahun',
        'Tanggal Register': 'tgl_register',
        'Lembaga Peradilan': 'lembaga_pengadilan',
        'Jenis Lembaga Peradilan': 'jenis_lembaga_peradilan',
        'Hakim Ketua': 'hakim_ketua',
        'Hakim Anggota': 'hakim_anggota',
        'Panitera': 'panitera',
        'Amar': 'amar',
        'Amar Lainnya': 'amar_lainnya',
        'Catatan Amar': 'catatan_amar',
        'Tanggal Musyawarah': 'tgl_musyawarah',
        'Tanggal Dibacakan': 'tgl_dibacakan',
    }
    
    data = {
        "link_page_putusan": url,
        "nama_terdakwa": nama_terdakwa,
        "link_pdf_putusan": pdf_url,
        "gcs_pdf_path": None     # diisi di upload step
    }
    
    # extract each col
    for col, out_field in col_map.items():
        try:
            target = soup.find("td", string=col)
            if target:
                if col == 'Klasifikasi':
                    result = (
                        target.find_next("td")
                        .get_text()
                        .replace(' \n ', ' - ')
                        .replace('  ', ' - ')
                        .strip()
                    )
                elif col in ['Tanggal Register', 'Tanggal Musyawarah', 'Tanggal Dibacakan']:
                    str_date = target.find_next("td").get_text().strip()
                    result = covert_tanggal_id_to_date(str_date)
                elif col == 'Tahun':
                    try:
                        result = int(target.find_next("td").get_text().strip())
                    except Exception:
                        result = None
                else:
                    result = (
                        target.find_next("td")
                        .get_text()
                        .strip()
                        .replace("\n", "")
                        .replace("  ", " ")
                        .replace(";", ";\n")
                    )
            else:
                result = None
        except Exception:
            result = None
        
        data[out_field] = result
    
    return pd.DataFrame([data])
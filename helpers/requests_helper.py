import requests
import random
import time
import os

headers = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}

def fetch_html_requests(url, logger, timeout=180, max_retries=5):
    """
    Fetch HTML using requests with retry logic.
    - Retry on network errors, timeouts, 5xx.
    - Stop immediately on 4xx (client error).
    """

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"[Attempt {attempt}] Fetching {url}")
            resp = requests.get(url, headers=headers, timeout=timeout)
            
            # 4xx client error -> langsung stop
            if 400 <= resp.status_code < 500:
                resp.raise_for_status()

            # 5xx server error -> retry
            if resp.status_code >= 500:
                raise requests.exceptions.HTTPError(f"Server error {resp.status_code}")

            html = resp.text.strip()
            if not html:
                raise ValueError("Empty response body")

            return html

        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError,
                ValueError) as e:

            if attempt < max_retries:
                sleep_time = random.randint(3, 8)
                logger.info(f"[Attempt {attempt}] Error: {e} -> retrying in {sleep_time}s")
                time.sleep(sleep_time)
                continue
            else:
                logger.error(f"Failed to fetch {url} after {max_retries} attempts")
                raise


def download_temp_file(url, local_path, logger, timeout=300, max_retries=5, chunk_size=8192):
    """
    Download file (PDF, etc.) with retry and exponential backoff.
    
    Args:
        url (str): URL sumber file.
        local_path (str): Path file lokal tempat simpan sementara (misalnya /tmp/file.pdf).
        logger: logger Airflow/Custom.
        timeout (int): timeout tiap request.
        max_retries (int): jumlah maksimal percobaan.
        chunk_size (int): ukuran buffer untuk streaming write.
    
    Returns:
        str: path file lokal yang berhasil didownload.
    """

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"[Attempt {attempt}] Downloading {url} -> {local_path}")
            with requests.get(url, headers=headers, stream=True, timeout=timeout) as resp:
                
                # 4xx client error -> langsung stop
                if 400 <= resp.status_code < 500:
                    resp.raise_for_status()

                # 5xx server error -> retry
                if resp.status_code >= 500:
                    raise requests.exceptions.HTTPError(f"Server error {resp.status_code}")

                with open(local_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=chunk_size):
                        if chunk:  # skip keep-alive
                            f.write(chunk)

            # verifikasi file non-empty
            if os.path.getsize(local_path) == 0:
                raise ValueError("Downloaded file is empty")

            logger.info(f"Downloaded successfully: {local_path}")
            return local_path

        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError,
                ValueError) as e:

            if attempt < max_retries:
                sleep_time = random.randint(5, 15)
                logger.warning(f"[Attempt {attempt}] Error: {e} -> retrying in {sleep_time}s")
                time.sleep(sleep_time)
                continue
            else:
                logger.error(f"Failed to download {url} after {max_retries} attempts")
                raise
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup


# Mapping for unit conversion
UNIT_MULTIPLIERS = {
    'rb': 1_000,
    'jt': 1_000_000,
    'm': 1_000_000_000,
    't': 1_000_000_000_000,
    'ribu': 1_000,
    'juta': 1_000_000,
    'milyar': 1_000_000_000,
    'triliun': 1_000_000_000_000,
}

def normalize_stat(stat: str, logger) -> int:
    """Convert a string like '4076 Ribu' into an integer."""
    try:
        if stat.replace(',', '').replace('.', '').isnumeric():
            return int(float(stat.replace(',', '')))
        num, unit = stat.split()
        multiplier = UNIT_MULTIPLIERS.get(unit.lower(), 1)
        return int(float(num.replace(',', '')) * multiplier)
    except Exception as e:
        logger.error(f"Error during scraping: {e}")
        raise

def get_stat_adapundi(logger):
    """Scrape and normalize statistics from Adapundi's About page."""
    url = 'https://www.adapundi.com/about'

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--ignore-ssl-errors=yes')
    options.add_argument('--ignore-certificate-errors')

    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
    options.add_argument(f'user-agent={user_agent}')

    remote_webdriver = "selenium"

    with webdriver.Remote(
        command_executor=f"http://{remote_webdriver}:4444/wd/hub",
        options=options
    ) as driver:

        for attempt in range(3):
            try:
                driver.get(url)
                WebDriverWait(driver, 3)

                soup = BeautifulSoup(driver.page_source, 'html.parser')
                containers = soup.find_all('div', class_='text-xl font-bold font-inter text-white')

                stats = [normalize_stat(con.get_text(strip=True), logger) for con in containers]

                if len(stats) == 10:
                    data = {
                        'penerima_dana_total': stats[0],
                        'penerima_dana_tahun_berjalan': stats[1],
                        'penerima_dana_posisi_akhir': stats[2],

                        'pemberi_dana_total': stats[7],
                        'pemberi_dana_tahun_berjalan': stats[8],
                        'pemberi_dana_posisi_akhir': stats[9],

                        'dana_tersalurkan_total': stats[3],
                        'dana_tersalurkan_tahun_berjalan': stats[4],
                        'dana_tersalurkan_posisi_akhir': stats[5]
                    }
                    logger.info(f"{url} scrape success")
                    return pd.DataFrame([data])

                driver.refresh()

            except Exception as e:
                logger.error(f"Error during scraping: {e}")
                raise

        logger.error(f"Statistics not found at {url} after 3 attempts.")
        raise

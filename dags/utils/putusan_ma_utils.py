import pendulum
import os
import pandas as pd
from helpers.requests_helper import download_temp_file
from helpers.discord_helper import discord_alert_message, discord_send_message
from helpers.dataframe_helper import cast_types_for_bq
from schemas.bigquery_schemas.putusan_ma_schema import (
    PUTUSAN_LINK_PAGE_SCHEMA,
    PUTUSAN_MA_SCHEMA,
    PUTUSAN_EXTRACT_PDF_SCHEMA,
)
from scripts.mahkamah_agung.crawl_putusan_ma import (
    scrape_putusan_links,
    scrape_detail_putusan,
)
from scripts.mahkamah_agung.extract_putusan_pdf import extract_data_putusan_pdf


def ensure_nltk_resources(logger):
    import nltk
    try:
        nltk.data.find("tokenizers/punkt")
    except LookupError:
        logger.info("Downloading NLTK punkt...")
        nltk.download("punkt")
    try:
        nltk.data.find("tokenizers/punkt_tab")
    except LookupError:
        logger.info("Downloading NLTK punkt_tab...")
        nltk.download("punkt_tab")
    try:
        nltk.data.find("corpora/stopwords")
    except LookupError:
        logger.info("Downloading NLTK stopwords...")
        nltk.download("stopwords")

def get_year_month_task(**kwargs):
    logical_date = kwargs["logical_date"]
    return logical_date.year, logical_date.month


def scrape_and_ingest_link_putusan_to_bq(year, month, logger, table):
    df = scrape_putusan_links(
        year=year,
        month=month,
        kategori="ite",
        pengadilan="pn-surabaya",
        logger=logger,
        max_retries=9,
        timeout=300,
    )
    
    df["run_date"] = pendulum.now().date()

    table.insert_df(
        df=df,
        schema=PUTUSAN_LINK_PAGE_SCHEMA,
        partition_field="tgl_putusan",
        if_exists="replace",
        run_date="run_date",
    )

    discord_send_message(title=f'link_putusan{year}-{month}', message=df.to_markdown(index=False), logger=logger)
    return len(df)


def scrape_and_ingest_putusan_detail_to_bq_and_upload_pdf_to_gcs(
    year, month, logger, link_page_table, stg_table, storage
):  
    start, end = pendulum.date(year, month, 1), pendulum.date(year, month, 1).add(months=1)
    query = f"""
        SELECT no_putusan, link_page_putusan, tgl_putusan
        FROM {link_page_table.full_table_id()}
        WHERE tgl_putusan >= '{start.to_date_string()}'
        AND tgl_putusan < '{end.to_date_string()}'
    """
    link_page_df = link_page_table.extract_to_df(query=query)
    result_dfs = []

    for _, row in link_page_df.iterrows():
        no_putusan, link_page = row["no_putusan"], row["link_page_putusan"]
        try:
            df_detail = scrape_detail_putusan(link_page, logger)

            if not df_detail.empty:
                link_pdf_putusan = df_detail.iloc[0].get("link_pdf_putusan")

                # upload pdf file jika ada
                if link_pdf_putusan and link_pdf_putusan != "no_pdf_file":
                    file_name = no_putusan.replace("/", "_").replace(' ', '_')

                    # 1. Path lokal (sementara di container)
                    local_pdf = f"/opt/data/tmp/{file_name}.pdf"

                    # 2. Download PDF ke lokal
                    download_temp_file(
                        url=link_pdf_putusan, 
                        local_path=local_pdf,
                        timeout=400,
                        max_retries=10, 
                        logger=logger
                    )

                    # 3. Upload ke GCS
                    gcs_path = f"putusan_ma/pdf/{year}/{file_name}.pdf"
                    storage.upload_file(local_pdf, gcs_path)

                    # 4. add gcs path to dataframe
                    df_detail.loc[0, "gcs_pdf_path"] = f"gs://{storage.bucket.name}/{gcs_path}"

                df_detail.loc[0, "no_putusan"] = no_putusan

                # 6. append hasil scraping detail putusan
                result_dfs.append(df_detail)

        except Exception as e:
            logger.error(f"Failed Extracting {no_putusan}: {e}", exc_info=True)
            raise

    if not result_dfs:
        logger.warning("No detail data scraped")
        return 0

    link_page_df = link_page_df.drop(columns=["link_page_putusan"], errors="ignore")

    result_df = pd.concat(result_dfs, ignore_index=True)
    merged_df = link_page_df.merge(result_df, on="no_putusan", how="left")
    
    stg_table.insert_df(
        df=merged_df,
        schema=PUTUSAN_MA_SCHEMA,
        partition_field="tgl_putusan",
        if_exists="replace",
        run_date="run_date",
    )

    discord_alert_message(title=None, detail=f'putusan_detail {len(merged_df)} rows', logger=logger, alert_type="warning")
    return len(merged_df)


def extract_ingest_upload_putusan_pdf_to_bq_gcs(
    year, month, logger, stg_table, stg_extract_table, storage
):
    start, end = pendulum.date(year, month, 1), pendulum.date(year, month, 1).add(months=1)
    query = f"""
        SELECT no_putusan, gcs_pdf_path, tgl_putusan
        FROM {stg_table.full_table_id()}
        WHERE tgl_putusan >= '{start.to_date_string()}'
        AND tgl_putusan < '{end.to_date_string()}'
    """
    link_df = stg_table.extract_to_df(query=query)
    result_dfs = []
    list_file_name = []

    for _, row in link_df.iterrows():
        no_putusan, gcs_pdf_path = row["no_putusan"], row["gcs_pdf_path"]
        logger.info(f"Extracting putusan: {no_putusan} from {gcs_pdf_path}")

        try:
            if gcs_pdf_path:
                file_name = no_putusan.replace("/", "_").replace(' ', '_')

                # 1. Path lokal (sementara di container) belum dihapus 
                local_pdf = f"/opt/data/tmp/{file_name}.pdf"

                # 2. Extract data from pdf (file di tmp)
                clean_text, df_detail = extract_data_putusan_pdf(pdf_path=local_pdf, logger=logger)

                # 3. Save text to .txt
                local_txt = f"/opt/data/tmp/{file_name}.txt"
                with open(local_txt, "w", encoding="utf-8") as f:
                    f.write(clean_text)

                # 4. Upload Extracted file .txt to GCS
                gcs_txt_path = f"putusan_ma/extract/{year}/{file_name}.txt"
                storage.upload_file(local_txt, gcs_txt_path)

                # 5. add gcs path (Extracted file .txt)
                df_detail.loc[0, "gcs_extract_pdf_path"] = f"gs://{storage.bucket.name}/{gcs_txt_path}"

                # 6. append hasil extract
                result_dfs.append(df_detail)

                # list file name
                list_file_name += [file_name]

        except Exception as e:
            logger.error(f"Failed extracting {no_putusan}: {e}", exc_info=True)
            raise

    if not result_dfs:
        logger.warning("No detail data extracted")
        return 0

    link_df = link_df.drop(columns=["gcs_pdf_path"], errors="ignore")

    result_df = pd.concat(result_dfs, ignore_index=True)
    merged_df = link_df.merge(result_df, on="no_putusan", how="left")

    merged_df = cast_types_for_bq(merged_df, PUTUSAN_EXTRACT_PDF_SCHEMA)

    stg_extract_table.insert_df(
        df=merged_df,
        schema=PUTUSAN_EXTRACT_PDF_SCHEMA,
        partition_field="tgl_putusan",
        if_exists="replace",
        run_date="run_date",
    )

    # Bersihkan file lokal
    # for file_name in list_file_name:
    #     try:
    #         os.remove(f"/opt/data/tmp/{file_name}.pdf")
    #         os.remove(f"/opt/data/tmp/{file_name}.txt")
    #     except Exception:
    #         pass

    discord_alert_message(title=None, detail=f'extract_pdf {len(merged_df)} rows', logger=logger, alert_type="warning")
    return len(merged_df)

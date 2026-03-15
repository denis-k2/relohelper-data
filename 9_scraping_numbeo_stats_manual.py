from __future__ import annotations

import argparse
import importlib.util
import logging
import re
from datetime import date
from os import getenv
from pathlib import Path
from time import time
from types import ModuleType
from typing import Optional

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

MANUAL_HTML_DIR = Path("./data/manual_numbeo_html")
MANUAL_LOG_FILE_PATH = "./data/logs_numbeo_city_costs_manual.log"


def setup_logging() -> None:
    logging.basicConfig(
        filename=MANUAL_LOG_FILE_PATH,
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def load_base_module() -> ModuleType:
    base_path = Path(__file__).with_name("9_scraping_numbeo_stats.py")
    spec = importlib.util.spec_from_file_location("numbeo_stats_base", base_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load base module from {base_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def extract_geoname_id_from_filename(path: Path) -> int:
    match = re.search(r"\d+", path.stem)
    if not match:
        raise ValueError(f"geonameid not found in filename: {path.name}")
    return int(match.group(0))


def scrape_numbeo_city_costs_manual(limit: Optional[int] = None) -> None:
    updated_by = getenv("DATA_ENGR")
    db_url = getenv("SQLALCHEMY_RELOHELPER_URL")
    updated_date = date.today()

    if not updated_by:
        raise RuntimeError("DATA_ENGR environment variable is not set")
    if not db_url:
        raise RuntimeError("SQLALCHEMY_RELOHELPER_URL environment variable is not set")

    setup_logging()
    start_time = time()

    if not MANUAL_HTML_DIR.exists():
        raise RuntimeError(f"Manual HTML directory not found: {MANUAL_HTML_DIR}")

    html_files = sorted(MANUAL_HTML_DIR.glob("*.html"))
    if limit is not None:
        html_files = html_files[:limit]

    base = load_base_module()
    df_summary_empty = base.create_df_summary_empty()

    success_count = 0
    fail_count = 0

    connection = None
    try:
        connection = base.connect_db(db_url)
        cursor = connection.cursor()

        for html_path in html_files:
            geoname_id = extract_geoname_id_from_filename(html_path)
            try:
                html = html_path.read_text(encoding="utf-8", errors="ignore")
                soup = base.get_soup(html)

                df_summary_complete = base.create_df_summary_complete(
                    soup, df_summary_empty
                )
                df_main_table = base.create_main_table(html)
                df_main_table = pd.concat(
                    [df_main_table, df_summary_complete],
                    ignore_index=True,
                )
                df_main_table = base.tidy_main_table(df_main_table)

                last_update = base.parse_last_update(soup)
                rows_to_insert = base.build_rows_for_insert(
                    cursor=cursor,
                    table=df_main_table,
                    geoname_id=geoname_id,
                    last_update=last_update,
                    updated_date=updated_date,
                    updated_by=updated_by,
                )

                base.insert_numbeo_city_costs(cursor, connection, rows_to_insert)
                success_count += 1
                logging.info(
                    "geoname_id=%s parsed from file=%s (rows=%s)",
                    geoname_id,
                    html_path.name,
                    len(rows_to_insert),
                )
            except Exception as ex:
                connection.rollback()
                fail_count += 1
                logging.exception(
                    "Manual parse failed for geoname_id=%s, file=%s, error=%s",
                    geoname_id,
                    html_path.name,
                    ex,
                )

        elapsed = time() - start_time
        logging.info("Finished manual parsing in %.2f sec", elapsed)
        logging.info("Success files: %s", success_count)
        logging.info("Failed files: %s", fail_count)

        print("[INFO] Manual Numbeo parsing finished.")
        print(f"[INFO] Success files: {success_count}")
        print(f"[INFO] Failed files: {fail_count}")
        print(f"[INFO] Execution time: {elapsed:.2f} sec")

    finally:
        if connection:
            connection.close()
            print("[INFO] Postgres connection closed.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only first N manual html files",
    )
    args = parser.parse_args()
    scrape_numbeo_city_costs_manual(limit=args.limit)


if __name__ == "__main__":
    main()

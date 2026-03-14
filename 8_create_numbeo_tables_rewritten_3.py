import logging
from io import StringIO
from os import getenv
from pathlib import Path
from time import sleep
from typing import Optional

import pandas as pd
import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2 import Error
from psycopg2.extensions import connection as PgConnection
from psycopg2.extensions import cursor as PgCursor

load_dotenv()

NUMBEO_SAMPLE_URL = (
    "https://www.numbeo.com/cost-of-living/in/New-York?displayCurrency=USD"
)
DEFAULT_TIMEOUT = 30
REQUEST_DELAY_SECONDS = 0.2
LOG_FILE_PATH = "./data/logs_create_numbeo_tables.log"

SUMMARY_PARAMS = [
    "The estimated monthly costs for a family of four",
    "The estimated monthly costs for a single person",
]


def setup_logging() -> None:
    logging.basicConfig(
        filename=LOG_FILE_PATH,
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


# =========================================================
# HTTP helpers
# =========================================================


def build_session() -> requests.Session:
    """Create a persistent session for Numbeo requests."""
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64; rv:148.0) Gecko/20100101 Firefox/148.0"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.numbeo.com/",
        }
    )
    return session


def fetch_html(session: requests.Session, url: str) -> str:
    """Download sample Numbeo page HTML."""
    sleep(REQUEST_DELAY_SECONDS)
    response = session.get(url, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()

    print(f"[INFO] Sample URL: {response.url}")
    print(f"[INFO] Status code: {response.status_code}")

    return response.text


# =========================================================
# Parsing helpers
# =========================================================


def find_main_numbeo_table(html: str) -> pd.DataFrame:
    """
    Find the main Numbeo price table.
    """
    try:
        tables = pd.read_html(StringIO(html))
    except ValueError as ex:
        raise ValueError(
            f"pd.read_html could not parse any tables. HTML preview: {html[:500]!r}"
        ) from ex

    print(f"[INFO] Tables found on sample page: {len(tables)}")

    for i, table in enumerate(tables):
        cols = [str(col).strip() for col in table.columns]
        print(f"[INFO] Table {i} columns: {cols}")

        col_set = set(cols)
        if {"Restaurants", "Edit", "Range"}.issubset(col_set):
            print(f"[INFO] Main Numbeo table selected: index {i}")
            return table.copy()

    if len(tables) >= 2:
        print("[INFO] Fallback: using table index 1")
        return tables[1].copy()

    raise ValueError("Could not find the main Numbeo table with expected columns")


def build_aux_table(main_table: pd.DataFrame) -> pd.DataFrame:
    """
    Convert the Numbeo main table into an auxiliary table with category labels.

    Original Numbeo layout stores category rows inside the same table.
    This function restores the category for each parameter row.
    """
    table = main_table.copy()

    temp_tbl = pd.DataFrame([table.columns], columns=table.columns)
    table = pd.concat([temp_tbl, table], ignore_index=True)

    current_category: Optional[str] = None
    cleaned_rows = []

    for _, row in table.iterrows():
        param_name = row["Restaurants"]
        edit_value = row["Edit"]

        if edit_value == "Edit":
            current_category = param_name
            continue

        row_dict = row.to_dict()
        row_dict["categories"] = current_category
        cleaned_rows.append(row_dict)

    result = pd.DataFrame(cleaned_rows)

    if result.empty:
        raise ValueError("Auxiliary Numbeo table is empty after category parsing")

    return result.reset_index(drop=True)


def extract_categories(aux_table: pd.DataFrame) -> list[str]:
    """Get unique category names in original order."""
    categories = []

    for item in aux_table["categories"].dropna().tolist():
        if item not in categories:
            categories.append(item)

    if "Summary" not in categories:
        categories.append("Summary")

    return categories


def extract_param_rows(aux_table: pd.DataFrame) -> list[tuple[str, str]]:
    """
    Extract parameter rows as (category, param).

    Summary parameters are appended manually because they are not part of the
    main price table body.
    """
    rows: list[tuple[str, str]] = []

    for _, row in aux_table.iterrows():
        category = row["categories"]
        param = row["Restaurants"]

        if pd.isna(category) or pd.isna(param):
            continue

        rows.append((str(category), str(param)))

    for param in SUMMARY_PARAMS:
        rows.append(("Summary", param))

    return rows


# =========================================================
# Database helpers
# =========================================================


def connect_db(db_url: str) -> PgConnection:
    return psycopg2.connect(db_url)


def create_numbeo_cost_categories_table(
    cursor: PgCursor, connection: PgConnection
) -> None:
    sql_file = (
        Path(__file__).resolve().parent / "sql" / "create_numbeo_cost_categories.sql"
    )
    cursor.execute(sql_file.read_text(encoding="utf-8"))
    connection.commit()


def create_numbeo_cost_params_table(cursor: PgCursor, connection: PgConnection) -> None:
    sql_file = Path(__file__).resolve().parent / "sql" / "create_numbeo_cost_params.sql"
    cursor.execute(sql_file.read_text(encoding="utf-8"))
    connection.commit()


def create_numbeo_stat_table(cursor: PgCursor, connection: PgConnection) -> None:
    sql_file = Path(__file__).resolve().parent / "sql" / "create_numbeo_city_costs.sql"
    cursor.execute(sql_file.read_text(encoding="utf-8"))
    connection.commit()


def insert_categories(
    cursor: PgCursor,
    connection: PgConnection,
    categories: list[str],
) -> None:
    for category in categories:
        cursor.execute(
            """
            INSERT INTO public.numbeo_cost_categories (category)
            VALUES (%s)
            ON CONFLICT (category) DO NOTHING
            """,
            (category,),
        )
    connection.commit()


def insert_params(
    cursor: PgCursor,
    connection: PgConnection,
    param_rows: list[tuple[str, str]],
) -> None:
    for category, param in param_rows:
        cursor.execute(
            """
            INSERT INTO public.numbeo_cost_params (category_id, param)
            VALUES (
                (SELECT category_id FROM public.numbeo_cost_categories WHERE category = %s),
                %s
            )
            ON CONFLICT (category_id, param) DO NOTHING
            """,
            (category, param),
        )
    connection.commit()


# =========================================================
# Main pipeline
# =========================================================


def create_numbeo_tables() -> None:
    db_url = getenv("SQLALCHEMY_RELOHELPER_URL")
    if not db_url:
        raise RuntimeError("SQLALCHEMY_RELOHELPER_URL environment variable is not set")

    setup_logging()
    session = build_session()

    connection: Optional[PgConnection] = None

    try:
        connection = connect_db(db_url)
        cursor = connection.cursor()

        create_numbeo_cost_categories_table(cursor, connection)
        create_numbeo_cost_params_table(cursor, connection)
        create_numbeo_stat_table(cursor, connection)

        html = fetch_html(session, NUMBEO_SAMPLE_URL)
        main_table = find_main_numbeo_table(html)
        aux_table = build_aux_table(main_table)

        categories = extract_categories(aux_table)
        param_rows = extract_param_rows(aux_table)

        insert_categories(cursor, connection, categories)
        insert_params(cursor, connection, param_rows)

        print("[INFO] Numbeo tables created/updated successfully")
        logging.info("Numbeo tables created/updated successfully")
        logging.info("Categories inserted: %s", len(categories))
        logging.info("Params inserted: %s", len(param_rows))

    except (requests.RequestException, ValueError, Error, Exception) as error:
        print("[INFO] Error:", error)
        logging.exception("Failed to create Numbeo tables: %s", error)
        if connection:
            connection.rollback()
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")


if __name__ == "__main__":
    create_numbeo_tables()

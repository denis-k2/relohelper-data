from __future__ import annotations

import argparse
import logging
from collections.abc import Hashable
from datetime import date, datetime
from io import StringIO
from os import getenv
from time import sleep, time
from typing import Any, Iterable, Optional

import pandas as pd
import psycopg2
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from psycopg2 import Error
from psycopg2.extensions import connection as PgConnection
from psycopg2.extensions import cursor as PgCursor

load_dotenv()

# =========================================================
# Configuration
# =========================================================

NUMBEO_LINKS_PICKLE_PATH = "./data/geonameid.pkl"
LOG_FILE_PATH = "./data/logs_numbeo_stats.log"
DEFAULT_TIMEOUT = 30
REQUEST_DELAY_SECONDS = 0.2

# Numbeo contains duplicated parameter names for imported beer.
# They must be mapped explicitly because a plain subquery by param name
# would be ambiguous.
SPECIAL_PARAM_IDS = {
    "Imported Beer (0.33 liter bottle)": 5,
    "Imported Beer (0.5 liter bottle)": 26,
}

SKIP_PARAMS = {
    "Domestic Draft Non-Alcoholic Beer (0.5 Liter)",
    "Domestic Non-Alcoholic Beer (0.5 Liter Bottle)",
    "Imported Non-Alcoholic Beer (0.33 Liter Bottle)",
    "Bottle of Non-Alcoholic Wine (Mid-Range)",
    "Buffalo Round or Equivalent Back Leg Red Meat (1 kg)",
}

SUMMARY_PARAM_NAMES = {
    "Family of four estimated monthly costs (without rent)",
    "A single person estimated monthly costs (without rent)",
}


# =========================================================
# Logging
# =========================================================


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


def get_response_text(session: requests.Session, link: str) -> str:
    """Fetch page HTML and fail loudly on HTTP errors."""
    sleep(REQUEST_DELAY_SECONDS)
    response = session.get(link, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    return response.text


def get_soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


# =========================================================
# Parsing helpers
# =========================================================


def create_df_summary_empty() -> pd.DataFrame:
    """Create a fixed template for Numbeo summary rows."""
    return pd.DataFrame(
        {
            "Restaurants": [
                "Summary",
                "Family of four estimated monthly costs (without rent)",
                "A single person estimated monthly costs (without rent)",
            ],
            "Edit": ["Edit", None, None],
            "Range": [None, None, None],
        }
    )


def extract_summary_costs(soup: BeautifulSoup) -> tuple[Optional[str], Optional[str]]:
    """
    Extract summary costs from the page.

    Numbeo sometimes changes formatting, so this parser is intentionally tolerant.
    It returns raw numeric strings or None.
    """
    try:
        div = soup.find(
            "div",
            class_=(
                "seeding-call table_color summary limit_size_ad_right "
                "padding_lower other_highlight_color"
            ),
        )
        if div is None:
            return None, None

        items = div.find_all("li")
        if len(items) < 2:
            return None, None

        first_item_tokens = items[0].get_text(" ", strip=True).split()
        second_item_tokens = items[1].get_text(" ", strip=True).split()

        family_costs = None
        person_costs = None

        for token in first_item_tokens:
            clean = token.strip("()")
            if clean.endswith("$"):
                family_costs = clean.rstrip("$")
                break

        for token in second_item_tokens:
            clean = token.strip("()")
            if clean.endswith("$"):
                person_costs = clean.rstrip("$")
                break

        return family_costs, person_costs

    except (AttributeError, IndexError, ValueError):
        return None, None


def create_df_summary_complete(
    soup: BeautifulSoup,
    df_summary_empty: pd.DataFrame,
) -> pd.DataFrame:
    """Fill the summary template with values parsed from the page."""
    family_costs, person_costs = extract_summary_costs(soup)

    df_summary_complete = df_summary_empty.copy()
    df_summary_complete.loc[1, "Edit"] = family_costs
    df_summary_complete.loc[2, "Edit"] = person_costs

    return df_summary_complete


def parse_last_update(soup: BeautifulSoup) -> Optional[date]:
    """Parse 'Last update: Month Year' from the page, if present."""
    try:
        div = soup.find("div", class_="align_like_price_table")
        if div is None:
            return None

        text = div.get_text("\n", strip=True)
        for line in text.splitlines():
            if line.startswith("Last update:"):
                raw = line.removeprefix("Last update:").strip()
                return datetime.strptime(raw, "%B %Y").date()

        return None
    except (AttributeError, ValueError):
        return None


def create_main_table(html: str) -> pd.DataFrame:
    """Read the main Numbeo price table from page HTML."""
    tables = pd.read_html(StringIO(html))
    for table in tables:
        cols = {str(col).strip() for col in table.columns}
        if {"Restaurants", "Edit", "Range"}.issubset(cols):
            return table.copy()

    if len(tables) < 2:
        raise ValueError("Expected at least 2 HTML tables on the Numbeo page")
    return tables[1].copy()


def parse_numeric_value(value: object) -> Optional[float]:
    """Convert a Numbeo numeric cell to float when possible."""
    if value is None:
        return None

    text = str(value).replace(",", "").replace("\xa0", " ").strip()
    text = text.strip("$")

    if text in {"", "?", "nan", "None"}:
        return None

    try:
        return float(text)
    except ValueError:
        return None


def parse_range_value(value: object) -> Optional[str]:
    """
    Convert a Numbeo range like '5.00-8.00' to a normalized string '[5.0, 8.0]'.
    Returns None if conversion fails.
    """
    if value is None:
        return None

    text = str(value).replace(",", "").strip()
    if text in {"", "?", "nan", "None"}:
        return None

    parts = [part.strip() for part in text.split("-")]
    if len(parts) != 2:
        return None

    try:
        normalized = [float(parts[0]), float(parts[1])]
        return str(normalized)
    except ValueError:
        return None


def tidy_main_table(table: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize Numbeo table values.

    Output columns keep the original names used by the project:
    - Restaurants : parameter name
    - Edit        : numeric value
    - Range       : price range as normalized string or None
    """
    table = table.copy()
    required_cols = {"Restaurants", "Edit", "Range"}
    if not required_cols.issubset(set(table.columns)):
        raise ValueError(f"Main table does not have expected columns: {table.columns}")

    table["Edit"] = table["Edit"].replace({"?": None})
    table["Edit"] = table["Edit"].apply(parse_numeric_value)
    table["Range"] = table["Range"].apply(parse_range_value)
    table["Restaurants"] = table["Restaurants"].astype("string").str.strip()
    table["Restaurants"] = table["Restaurants"].replace(
        {"": None, "nan": None, "None": None}
    )

    table = table.where(pd.notnull(table), None)
    table = table.dropna(subset=["Restaurants"])
    table = table.loc[table["Restaurants"] != "Restaurants"]
    table = table.loc[table["Edit"] != "Edit"]

    return table.reset_index(drop=True)


# =========================================================
# Database helpers
# =========================================================


def connect_db(db_url: str) -> PgConnection:
    return psycopg2.connect(db_url)


def resolve_city_id(row_index: int, row: pd.Series) -> int:
    """
    Resolve city_id safely.

    Prefer explicit 'city_id' column. Fall back to DataFrame index for backward
    compatibility with older pickle files where city_id was stored as index.
    """
    if "city_id" in row.index:
        city_id_value: object = row.get("city_id")
        if isinstance(city_id_value, (pd.Series, pd.DataFrame)):
            return int(row_index)

        if city_id_value is None:
            return int(row_index)

        if isinstance(city_id_value, str):
            clean = city_id_value.strip()
            if clean:
                return int(clean)
            return int(row_index)

        if isinstance(city_id_value, bool):
            return int(row_index)

        if isinstance(city_id_value, int):
            return city_id_value

        if isinstance(city_id_value, float):
            if pd.isna(city_id_value):
                return int(row_index)
            return int(city_id_value)

        # Last-resort conversion for scalar-like values (e.g. numpy numbers).
        city_id_any: Any = city_id_value
        return int(city_id_any)
    return int(row_index)


def get_param_id(cursor: PgCursor, param_name: str) -> Optional[int]:
    """Resolve param_id by param text from numbeo_param table."""
    if param_name in SPECIAL_PARAM_IDS:
        return SPECIAL_PARAM_IDS[param_name]

    cursor.execute(
        "SELECT param_id FROM numbeo_param WHERE param = %s LIMIT 1",
        (param_name,),
    )
    result = cursor.fetchone()
    return None if result is None else int(result[0])


def normalize_param_name(value: object) -> Optional[str]:
    """Convert a raw table cell to a valid param name or return None."""
    if value is None:
        return None

    if isinstance(value, float) and pd.isna(value):
        return None

    if not isinstance(value, str):
        return None

    name = value.strip()
    if not name or name.lower() in {"nan", "none"}:
        return None
    return name


def normalize_link(value: object) -> str:
    """Convert DataFrame link cell to a non-empty URL string."""
    if isinstance(value, str):
        link = value.strip()
        if link:
            return link
    raise ValueError(f"Invalid link value: {value!r}")


def build_rows_for_insert(
    cursor: PgCursor,
    table: pd.DataFrame,
    city_id: int,
    last_update: Optional[date],
    updated_date: date,
    updated_by: str,
) -> list[tuple]:
    """Convert parsed table rows to insert tuples for numbeo_stat."""
    rows_to_insert: list[tuple] = []

    for _, row in table.iterrows():
        param_name = normalize_param_name(row["Restaurants"])
        if param_name is None:
            logging.warning(
                "Invalid param name for city_id=%s, row=%s", city_id, row.to_dict()
            )
            continue

        if param_name in SKIP_PARAMS:
            continue

        param_id = get_param_id(cursor, param_name)
        if param_id is None:
            logging.warning(
                "param_id not found for city_id=%s, param=%s",
                city_id,
                param_name,
            )
            continue

        rows_to_insert.append(
            (
                city_id,
                param_id,
                row["Edit"],
                row["Range"],
                last_update,
                updated_date,
                updated_by,
            )
        )

    return rows_to_insert


def insert_numbeo_stats(
    cursor: PgCursor,
    connection: PgConnection,
    rows_to_insert: Iterable[tuple],
) -> None:
    """
    Insert a city batch into numbeo_stat.

    Column naming follows the current project convention:
    last_update, updated_date, updated_by
    """
    insert_sql = """
        INSERT INTO numbeo_stat (
            city_id,
            param_id,
            cost,
            range,
            last_update,
            updated_date,
            updated_by
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (city_id, param_id)
        DO UPDATE SET
            cost = EXCLUDED.cost,
            range = EXCLUDED.range,
            last_update = EXCLUDED.last_update,
            updated_date = EXCLUDED.updated_date,
            updated_by = EXCLUDED.updated_by
    """

    rows_to_insert = list(rows_to_insert)
    if not rows_to_insert:
        return

    cursor.executemany(insert_sql, rows_to_insert)
    connection.commit()


# =========================================================
# Data loading
# =========================================================


def load_source_dataframe(limit: Optional[int] = None) -> pd.DataFrame:
    """Load source DataFrame from pickle and optionally limit row count."""
    raw: Any = pd.read_pickle(NUMBEO_LINKS_PICKLE_PATH)
    if not isinstance(raw, pd.DataFrame):
        raise TypeError(
            f"Expected DataFrame in pickle, got {type(raw).__name__}: {NUMBEO_LINKS_PICKLE_PATH}"
        )
    df = raw.copy()

    if "country" in df.columns:
        df = df.sort_values(by=["country"]).copy()

    if limit is not None:
        df = df.head(limit).copy()

    return df


def normalize_row_index(row_index: Hashable) -> int:
    """Convert DataFrame index value to int for legacy city_id fallback."""
    if isinstance(row_index, bool):
        raise TypeError(f"Boolean row index is not supported: {row_index!r}")
    if isinstance(row_index, int):
        return row_index
    if isinstance(row_index, str):
        return int(row_index.strip())
    if isinstance(row_index, float):
        if pd.isna(row_index):
            raise TypeError("NaN row index is not supported")
        return int(row_index)

    raise TypeError(f"Unsupported row index type: {type(row_index).__name__}")


# =========================================================
# Main pipeline
# =========================================================


def process_city(
    session: requests.Session,
    cursor: PgCursor,
    connection: PgConnection,
    row_index: int,
    row: pd.Series,
    df_summary_empty: pd.DataFrame,
    updated_date: date,
    updated_by: str,
) -> bool:
    """Scrape one city page and write parsed Numbeo stats to DB."""
    city_id = resolve_city_id(row_index, row)
    link = normalize_link(row.get("link"))

    try:
        html = get_response_text(session, link)
        soup = get_soup(html)

        df_summary_complete = create_df_summary_complete(soup, df_summary_empty)
        df_main_table = create_main_table(html)
        df_main_table = pd.concat(
            [df_main_table, df_summary_complete],
            ignore_index=True,
        )
        df_main_table = tidy_main_table(df_main_table)

        last_update = parse_last_update(soup)
        rows_to_insert = build_rows_for_insert(
            cursor=cursor,
            table=df_main_table,
            city_id=city_id,
            last_update=last_update,
            updated_date=updated_date,
            updated_by=updated_by,
        )

        insert_numbeo_stats(cursor, connection, rows_to_insert)
        logging.info("city_id=%s parsed successfully: %s", city_id, link)
        return True

    except requests.RequestException as ex:
        connection.rollback()
        logging.exception("HTTP error for city_id=%s, link=%s: %s", city_id, link, ex)
        return False
    except (ValueError, KeyError, IndexError) as ex:
        connection.rollback()
        logging.exception(
            "Parsing error for city_id=%s, link=%s: %s", city_id, link, ex
        )
        return False
    except Exception as ex:
        connection.rollback()
        logging.exception(
            "Unexpected error for city_id=%s, link=%s, error_type=%s: %s",
            city_id,
            link,
            type(ex).__name__,
            ex,
        )
        return False


def scrape_numbeo_stats(limit: Optional[int] = None) -> None:
    """End-to-end pipeline for scraping Numbeo city stats."""
    updated_by = getenv("DATA_ENGR")
    db_url = getenv("SQLALCHEMY_RELOHELPER_URL")
    updated_date = date.today()

    if not updated_by:
        raise RuntimeError("DATA_ENGR environment variable is not set")
    if not db_url:
        raise RuntimeError("SQLALCHEMY_RELOHELPER_URL environment variable is not set")

    setup_logging()
    start_time = time()

    df = load_source_dataframe(limit=limit)
    df_summary_empty = create_df_summary_empty()
    session = build_session()

    connection: Optional[PgConnection] = None
    success_count = 0
    fail_count = 0

    try:
        connection = connect_db(db_url)
        cursor = connection.cursor()

        for row_index, row in df.iterrows():
            row_index_int = normalize_row_index(row_index)
            ok = process_city(
                session=session,
                cursor=cursor,
                connection=connection,
                row_index=row_index_int,
                row=row,
                df_summary_empty=df_summary_empty,
                updated_date=updated_date,
                updated_by=updated_by,
            )
            if ok:
                success_count += 1
            else:
                fail_count += 1

        elapsed = time() - start_time
        logging.info("Finished scraping in %.2f sec", elapsed)
        logging.info("Success cities: %s", success_count)
        logging.info("Failed cities: %s", fail_count)

        print("[INFO] Numbeo scraping finished.")
        print(f"[INFO] Success cities: {success_count}")
        print(f"[INFO] Failed cities: {fail_count}")
        print(f"[INFO] Execution time: {elapsed:.2f} sec")

    except (Exception, Error) as error:
        print("[INFO Error]:", error)
        logging.exception("Fatal pipeline error: %s", error)

    finally:
        if connection:
            connection.close()
            print("[INFO] Postgres connection closed.")


# =========================================================
# CLI
# =========================================================


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only the first N cities (useful for testing)",
    )
    args = parser.parse_args()

    scrape_numbeo_stats(limit=args.limit)


if __name__ == "__main__":
    main()

import logging
from os import getenv
from typing import Optional

import pandas as pd
import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2 import Error
from psycopg2.extensions import connection as PgConnection
from psycopg2.extensions import cursor as PgCursor

load_dotenv()

# =========================================================
# Configuration
# =========================================================

NUMBEO_LINKS_PICKLE_PATH = "./data/geonameid.pkl"
LOG_FILE_PATH = "./data/logs_create_numbeo_tables.log"
DEFAULT_TIMEOUT = 30

SUMMARY_CATEGORY = "Summary"
SUMMARY_PARAMS = [
    "Family of four estimated monthly costs (without rent)",
    "A single person estimated monthly costs (without rent)",
]


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
# HTTP / parsing helpers
# =========================================================


def build_session() -> requests.Session:
    """Create a persistent session for Numbeo requests."""
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64; rv:148.0) "
                "Gecko/20100101 Firefox/148.0"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.numbeo.com/",
        }
    )
    return session



def get_sample_link() -> str:
    """Load one Numbeo city link from the source dataframe."""
    df = pd.read_pickle(NUMBEO_LINKS_PICKLE_PATH)

    if "link" not in df.columns:
        raise ValueError("Column 'link' was not found in geonameid.pkl")

    link_series = df["link"].dropna()
    if link_series.empty:
        raise ValueError("No valid Numbeo links were found in geonameid.pkl")

    return str(link_series.iloc[0])



def fetch_numbeo_tables(session: requests.Session, link: str) -> list[pd.DataFrame]:
    """Fetch HTML page and parse all HTML tables from it."""
    response = session.get(link, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    return pd.read_html(response.text)



def build_param_source_table(link: str) -> pd.DataFrame:
    """
    Build a flat dataframe with two columns:
    - categories
    - Restaurants (parameter name)

    Numbeo uses category headers embedded into the table itself.
    This function normalizes them into a separate 'categories' column.
    """
    session = build_session()
    tables = fetch_numbeo_tables(session, link)

    if len(tables) < 2:
        raise ValueError("Expected at least 2 HTML tables on the Numbeo page")

    table = tables[1].copy()

    # Numbeo stores one logical category in the table header and the rest in rows.
    # The original project solved this by prepending the header row as data.
    temp_tbl = pd.DataFrame([table.columns], columns=table.columns)
    table = pd.concat([temp_tbl, table], ignore_index=True)

    current_category: Optional[str] = None
    rows_to_keep = []

    for _, row in table.iterrows():
        param_name = row["Restaurants"]
        edit_value = row["Edit"]

        if edit_value == "Edit":
            current_category = str(param_name)
            continue

        rows_to_keep.append(
            {
                "categories": current_category,
                "Restaurants": param_name,
            }
        )

    result = pd.DataFrame(rows_to_keep)
    result = result.dropna(subset=["categories", "Restaurants"])
    result = result.drop_duplicates().reset_index(drop=True)

    return result


# =========================================================
# Database helpers
# =========================================================


def connect_db(db_url: str) -> PgConnection:
    return psycopg2.connect(db_url)



def create_numbeo_category_table(cursor: PgCursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS public.numbeo_category (
            category_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            category varchar(100) NOT NULL UNIQUE
        )
        """
    )



def create_numbeo_param_table(cursor: PgCursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS public.numbeo_param (
            param_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            category_id integer NOT NULL,
            param varchar(200) NOT NULL,
            CONSTRAINT fk_numbeo_param_category_id
                FOREIGN KEY (category_id)
                REFERENCES public.numbeo_category (category_id),
            CONSTRAINT uq_numbeo_param_category_param
                UNIQUE (category_id, param)
        )
        """
    )



def create_numbeo_stat_table(cursor: PgCursor) -> None:
    """
    Create the main Numbeo statistics table.

    Column naming follows the current project convention:
    last_update, updated_date, updated_by
    """
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS public.numbeo_stat (
            city_id integer NOT NULL,
            param_id integer NOT NULL,
            cost numeric,
            range varchar(50),
            last_update date,
            updated_date date,
            updated_by varchar(30),
            CONSTRAINT pk_numbeo_stat PRIMARY KEY (city_id, param_id),
            CONSTRAINT fk_numbeo_stat_city_id
                FOREIGN KEY (city_id)
                REFERENCES public.cities (city_id),
            CONSTRAINT fk_numbeo_stat_param_id
                FOREIGN KEY (param_id)
                REFERENCES public.numbeo_param (param_id)
        )
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_numbeo_stat_param_id
        ON public.numbeo_stat (param_id)
        """
    )



def insert_categories(cursor: PgCursor, table: pd.DataFrame) -> None:
    insert_sql = """
        INSERT INTO public.numbeo_category (category)
        VALUES (%s)
        ON CONFLICT (category) DO NOTHING
    """

    categories = sorted(set(table["categories"].dropna().astype(str).tolist()))
    if SUMMARY_CATEGORY not in categories:
        categories.append(SUMMARY_CATEGORY)

    for category in categories:
        cursor.execute(insert_sql, (category,))



def insert_params(cursor: PgCursor, table: pd.DataFrame) -> None:
    insert_sql = """
        INSERT INTO public.numbeo_param (category_id, param)
        VALUES (
            (SELECT category_id FROM public.numbeo_category WHERE category = %s),
            %s
        )
        ON CONFLICT (category_id, param) DO NOTHING
    """

    for _, row in table.iterrows():
        cursor.execute(insert_sql, (str(row["categories"]), str(row["Restaurants"])))

    for summary_param in SUMMARY_PARAMS:
        cursor.execute(insert_sql, (SUMMARY_CATEGORY, summary_param))


# =========================================================
# Main pipeline
# =========================================================


def create_numbeo_tables() -> None:
    db_url = getenv("SQLALCHEMY_RELOHELPER_URL")
    if not db_url:
        raise RuntimeError("SQLALCHEMY_RELOHELPER_URL environment variable is not set")

    setup_logging()
    connection: Optional[PgConnection] = None

    try:
        sample_link = get_sample_link()
        logging.info("Using sample Numbeo link: %s", sample_link)

        param_source_table = build_param_source_table(sample_link)

        connection = connect_db(db_url)
        cursor = connection.cursor()

        create_numbeo_category_table(cursor)
        create_numbeo_param_table(cursor)
        create_numbeo_stat_table(cursor)
        insert_categories(cursor, param_source_table)
        insert_params(cursor, param_source_table)

        connection.commit()
        print("[INFO] Numbeo tables created / updated successfully")
        logging.info("Numbeo tables created / updated successfully")

    except (Exception, Error) as error:
        if connection:
            connection.rollback()
        print("[INFO] Error:", error)
        logging.exception("Failed to create Numbeo tables: %s", error)

    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")


if __name__ == "__main__":
    create_numbeo_tables()
